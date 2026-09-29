WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        DATE_FORMAT(signup_date, 'yyyy-MM-dd') AS signup_date_label
    FROM customers
    WHERE signup_date >= DATE '2023-08-25'
      AND city IS NOT NULL
      AND UPPER(TRIM(email)) = 'CAROL@COMPANY.CO'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(credit_limit) OVER w_running AS running_credit_limit,
        FIRST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit,
        FIRST_VALUE(age, TRUE) OVER w_hist AS first_known_age,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER w_ordered AS last_known_credit_limit
    FROM valid
    WINDOW w_running AS (PARTITION BY city, country ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_ordered AS (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, segment
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id, segment
    FROM customers
    WHERE city <> 'Tokyo'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.segment = k.segment
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.status = 'EXPIRED'
        )
),
eligible_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date >= s.signup_date
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.status IN ('SHIPPED', 'PENDING')
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(running_credit_limit) OVER w_ordered AS segment_min_running_credit_limit,
        MIN(last_known_credit_limit) OVER w_ordered AS segment_min_last_known_credit_limit
    FROM not_flagged
    WINDOW w_ordered AS (PARTITION BY segment)
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        segment,
        CASE WHEN age >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
        SUM(running_credit_limit) FILTER (WHERE country = 'US') AS us_total_running_credit_limit,
        COUNT(*) AS event_count,
        COUNT_IF(country = 'DE') AS de_events,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) AS cnt,
        COUNT(*) AS row_count,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY SUM(running_credit_limit) FILTER (WHERE country = 'US') DESC) AS us_total_running_credit_limit_dense_rank
    FROM scored
    GROUP BY segment, CASE WHEN age >= 73 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    segment,
    age_tier,
    us_total_running_credit_limit,
    de_events,
    distinct_ids,
    cnt,
    row_count,
    us_total_running_credit_limit_dense_rank,
    CASE WHEN segment RLIKE '^(Corporate|SMB)$' THEN 'PRIORITY' WHEN segment RLIKE '^(Consumer)$' THEN 'GROUP_B' ELSE 'CORE' END AS segment_zone
FROM daily;
