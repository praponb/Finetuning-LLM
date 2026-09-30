WITH valid AS (
    SELECT
        *,
        CONCAT_WS('/', city, name) AS city_name_key,
        UPPER(TRIM(email)) AS email_normalized,
        COALESCE(city, 'NONE') AS city_filled
    FROM customers
    WHERE signup_date > DATE '2023-08-26'
      AND segment IS NOT NULL
),
eligible AS (
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.status = 'CANCELLED'
        )
),
enriched AS (
    SELECT
        id,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        email_normalized,
        ROUND(age / SUM(age) OVER (PARTITION BY country), 4) AS age_share,
        LAG(age, 2) OVER (PARTITION BY segment ORDER BY age, id) AS prev_age,
        MAX(age) OVER (PARTITION BY country) AS country_max_age,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date, id) AS row_seq,
        CUME_DIST() OVER (PARTITION BY segment, country ORDER BY age DESC) AS age_cume_dist
    FROM eligible
),
with_windows AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY city, segment ORDER BY age_cume_dist DESC) AS age_cume_dist_percent_rank,
        SUM(row_seq) OVER (PARTITION BY segment ORDER BY age_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        RANK() OVER (PARTITION BY segment ORDER BY country_max_age DESC NULLS LAST) AS country_max_age_rank,
        AVG(prev_age) OVER w_recent AS prev_age_avg_2
    FROM enriched
    WINDOW w_recent AS (PARTITION BY city, segment ORDER BY age_cume_dist, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    segment,
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    ROUND(AVG(cumulative_row_seq), 2) AS avg_cumulative_row_seq,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT segment) AS distinct_segments,
    AVG(age_cume_dist) FILTER (WHERE segment = 'Consumer') AS consumer_avg_age_cume_dist,
    GROUPING_ID() AS grouping_level
FROM with_windows
GROUP BY GROUPING SETS ((segment, DATE_TRUNC('MONTH', signup_date)), (segment), ())
ORDER BY consumer_avg_age_cume_dist NULLS LAST;
