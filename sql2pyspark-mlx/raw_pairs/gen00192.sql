WITH base AS (
    SELECT
        *,
        CONCAT(email, ', ', city) email_city,
        COALESCE(segment, 'NONE') resolved_segment
    FROM customers
    WHERE segment = 'Consumer'
),
enriched AS (
    SELECT
        id,
        name,
        email,
        country,
        city,
        credit_limit,
        SUM(age) OVER w_hist last3_sum_age,
        MIN(age) OVER w_hist last3_min_age,
        COUNT(age) OVER w_hist last3_count_age,
        AVG(age) OVER w_hist last3_avg_age,
        CUME_DIST() OVER w_hist_2 age_cume_dist
    FROM base
    WINDOW w_hist AS (PARTITION BY segment, city ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_hist_2 AS (PARTITION BY country ORDER BY age DESC)
),
latest_match AS (
    SELECT
        s.id,
        s.name,
        s.city,
        s.last3_sum_age,
        s.age_cume_dist,
        latest.opened_date,
        latest.account_type
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.opened_date, c.account_type
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at < s.opened_date
        )
),
latest AS (
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY account_type ORDER BY opened_date DESC NULLS FIRST, id) = 1
)
SELECT *
FROM (
    SELECT
        id,
        account_type,
        age_cume_dist,
        last3_sum_age
    FROM latest
) src
UNPIVOT (
    metric_value FOR measure IN (age_cume_dist, last3_sum_age)
)
