WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, v
),
enriched AS (
    -- Attach customers attributes
    SELECT
        a.*,
        b.name,
        b.segment,
        b.country
    FROM flattened a
    LEFT JOIN customers b
        ON a.customer_id = b.id
),
enriched_carts AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.name AS customer_name,
        b.segment AS customer_segment,
        b.age
    FROM enriched src
    LEFT JOIN customers b
        ON src.customer_id = b.id
    WHERE b.credit_limit IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        k,
        v,
        name,
        customer_name,
        customer_segment,
        LAG(age, 1) OVER w_hist AS prev_age,
        SUM(age) OVER (PARTITION BY customer_segment ORDER BY age, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_age,
        FIRST_VALUE(country, TRUE) OVER w_part AS first_known_country,
        LAST_VALUE(segment) IGNORE NULLS OVER w_grp AS last_known_segment,
        SUM(age) OVER w_grp AS cumulative_age_2
    FROM enriched_carts
    WINDOW w_hist AS (ORDER BY age, id, k), w_part AS (ORDER BY age, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY k ORDER BY age, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        k,
        customer_segment,
        prev_age,
        cumulative_age,
        COUNT(cumulative_age) OVER w_hist AS running_count_cumulative_age,
        AVG(cumulative_age) OVER w_hist AS running_avg_cumulative_age,
        MIN(cumulative_age) OVER w_hist AS running_min_cumulative_age,
        MAX(cumulative_age) OVER w_hist AS running_max_cumulative_age,
        LEAD(cumulative_age, 1) OVER (PARTITION BY customer_segment ORDER BY cumulative_age_2, id, k) AS next_cumulative_age
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY k ORDER BY cumulative_age, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
metrics_carts AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(cumulative_age) OVER (PARTITION BY k, customer_segment ORDER BY prev_age, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_age,
        ROW_NUMBER() OVER (ORDER BY prev_age DESC, id, k) AS row_seq,
        SUM(running_max_cumulative_age) OVER w_grp AS k_sum_running_max_cumulative_age
    FROM metrics
    WINDOW w_grp AS (PARTITION BY k, customer_segment)
),
totals AS (
    -- Multi-level aggregation
    SELECT
        k,
        CASE WHEN next_cumulative_age >= 304 THEN 'LARGE' ELSE 'SMALL' END AS next_cumulative_age_tier,
        AVG(next_cumulative_age) FILTER (WHERE k = 'gift') AS gift_avg_next_cumulative_age,
        COUNT(*) AS event_count,
        SIZE(COLLECT_SET(k)) AS num_ks
    FROM metrics_carts
    GROUP BY CUBE (k, CASE WHEN next_cumulative_age >= 304 THEN 'LARGE' ELSE 'SMALL' END)
    HAVING SIZE(COLLECT_SET(k)) > 1
)
SELECT
    k,
    next_cumulative_age_tier,
    num_ks,
    CASE WHEN k RLIKE '^(weight_kg|utm_source)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS k_zone
FROM totals
ORDER BY k_zone
