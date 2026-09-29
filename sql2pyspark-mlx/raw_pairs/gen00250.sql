WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN is_active THEN 'Yes' ELSE 'No' END is_active_label
    FROM customers
    WHERE signup_date > TO_DATE('2023-02-28')
      AND country = 'JP'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(age) OVER (PARTITION BY country ORDER BY signup_date, id) prev_age,
        LEAD(age) OVER (PARTITION BY country ORDER BY signup_date, id) next_age,
        LEAD(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id) next_credit_limit,
        LAG(credit_limit) OVER (PARTITION BY country ORDER BY signup_date, id) prev_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY signup_date, id) row_seq
    FROM cleaned
),
latest_match AS (
    -- Latest matching session per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.session_started_at,
        top1.session_country
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.started_at AS session_started_at, c.country AS session_country
            FROM sessions c
            WHERE c.customer_id = s.id
            ORDER BY c.started_at DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        email,
        country,
        signup_date,
        credit_limit,
        age,
        is_active_label,
        prev_credit_limit,
        session_country,
        AVG(next_credit_limit) OVER (PARTITION BY city, segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_next_credit_limit,
        MAX(next_credit_limit) OVER (PARTITION BY city, segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_next_credit_limit,
        MIN(next_credit_limit) OVER (PARTITION BY city, segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_next_credit_limit
    FROM latest_match
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.shipping_fee >= 31
        )
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN country = 'DE' THEN 1 END), 0), 2) AS credit_limit_per_de,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS credit_limit_per_sg
    FROM eligible
    GROUP BY 1
)
SELECT
    quarter_start,
    credit_limit_per_de,
    credit_limit_per_sg
FROM daily
ORDER BY credit_limit_per_sg NULLS LAST;
