WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(email, ' ', country) AS email_country,
        CASE WHEN segment = 'Corporate' THEN 1 ELSE 0 END AS is_corporate,
        COALESCE(segment, 'UNKNOWN') AS resolved_segment
    FROM customers
    WHERE age < 40
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        email,
        country,
        signup_date,
        is_active,
        credit_limit,
        age,
        resolved_segment,
        ROW_NUMBER() OVER w_part AS row_seq,
        SUM(is_corporate) OVER (PARTITION BY YEAR(signup_date) ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_is_corporate,
        LAST_VALUE(credit_limit) IGNORE NULLS OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_credit_limit
    FROM base
    WHERE age < 49
    WINDOW w_part AS (PARTITION BY country ORDER BY is_corporate, id)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        email,
        country,
        signup_date,
        credit_limit,
        row_seq,
        running_is_corporate,
        last_known_credit_limit
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS rank_in_group
        FROM scored
    ) t
    WHERE rank_in_group = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND YEAR(c.start_date) >= 2024
        )
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', signup_date) AS year_start,
        COUNT_IF(country = 'SG') AS sg_events,
        SUM(last_known_credit_limit) AS total_last_known_credit_limit,
        PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
        GROUPING_ID() AS grouping_level
    FROM kept
    GROUP BY ROLLUP (DATE_TRUNC('YEAR', signup_date))
    HAVING sg_events < 1000
)
SELECT
    sg_events,
    total_last_known_credit_limit,
    grouping_level
FROM summary
