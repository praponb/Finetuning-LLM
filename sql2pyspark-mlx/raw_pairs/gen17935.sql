WITH metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(credit_limit) OVER (PARTITION BY city) AS city_count_credit_limit,
        AVG(age) OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS age_avg_5_prior,
        AVG(credit_limit) OVER w_running AS credit_limit_avg_5
    FROM customers
    WINDOW w_running AS (PARTITION BY MONTH(signup_date), country ORDER BY signup_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY city_count_credit_limit DESC NULLS FIRST, id) AS dedup_rank
        FROM metrics
    ) t
    WHERE dedup_rank = 1
),
cleaned AS (
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
        city_count_credit_limit,
        age_avg_5_prior,
        credit_limit_avg_5,
        dedup_rank,
        SUBSTRING(segment, 1, 2) AS segment_prefix,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM ranked
    WHERE city IS NOT NULL
      AND credit_limit < 13100
),
metrics_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(credit_limit_avg_5, TRUE) OVER (PARTITION BY country ORDER BY city_count_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit_avg_5,
        CUME_DIST() OVER (PARTITION BY MONTH(signup_date), city ORDER BY age_avg_5_prior DESC NULLS FIRST) AS age_avg_5_prior_cume_dist
    FROM cleaned
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM metrics_customers s
    WHERE EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.device <> 'mobile'
        )
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        age,
        city_count_credit_limit,
        age_avg_5_prior,
        credit_limit_avg_5,
        dedup_rank,
        segment_prefix,
        segment_filled,
        last_known_credit_limit_avg_5,
        age_avg_5_prior_cume_dist
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY age_avg_5_prior_cume_dist, id) = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT segment, country
    FROM deduped
    EXCEPT
    SELECT segment, country
    FROM customers
    WHERE credit_limit <= 9500
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.segment = k.segment AND s.country = k.country
)
SELECT
    id,
    email,
    country,
    city,
    segment,
    age,
    age_avg_5_prior,
    segment_prefix,
    last_known_credit_limit_avg_5
FROM kept_rows
ORDER BY age
