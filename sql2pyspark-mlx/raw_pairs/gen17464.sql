WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        IF(is_active, 'Yes', 'No') AS is_active_label,
        CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label_2,
        COALESCE(segment, 'UNKNOWN') AS segment_filled
    FROM customers
    WHERE country = 'TH'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY signup_date DESC, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        country,
        city,
        segment,
        is_active_label,
        row_num,
        LAG(credit_limit) OVER (PARTITION BY segment, city ORDER BY age, id) AS prev_credit_limit,
        LEAD(credit_limit) OVER (PARTITION BY segment, city ORDER BY age, id) AS next_credit_limit,
        LAG(age) OVER (PARTITION BY segment, city ORDER BY age, id) AS prev_age,
        LEAD(age) OVER (PARTITION BY segment, city ORDER BY age, id) AS next_age,
        ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY age, id) AS event_seq
    FROM latest
    WHERE name LIKE '%Che%'
),
scored_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(prev_age) OVER (PARTITION BY country ORDER BY next_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_prev_age,
        SUM(prev_age) OVER (PARTITION BY country ORDER BY next_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_prev_age,
        MAX(prev_age) OVER (PARTITION BY country ORDER BY next_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_prev_age,
        MIN(prev_age) OVER (PARTITION BY country ORDER BY next_credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_prev_age
    FROM scored
)
-- Multi-level aggregation
SELECT
    city,
    SUM(prev_age) AS total_prev_age,
    APPROX_PERCENTILE(running_avg_prev_age, ARRAY(0.25, 0.5, 0.75)) AS running_avg_prev_age_quartiles,
    GROUPING(city) AS grouping_level
FROM scored_customers
GROUP BY CUBE (city);
