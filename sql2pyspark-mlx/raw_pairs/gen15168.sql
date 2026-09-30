WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(name) AS name_lower,
        COALESCE(city, 'DIRECT') AS resolved_city
    FROM customers
    WHERE credit_limit BETWEEN 13800 AND 14200
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        country,
        city,
        signup_date,
        is_active,
        credit_limit,
        name_lower,
        AVG(credit_limit) OVER (PARTITION BY city) AS city_avg_credit_limit,
        DENSE_RANK() OVER w_ordered AS credit_limit_dense_rank,
        SUM(age) OVER (PARTITION BY country, segment ORDER BY credit_limit, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_age
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY city ORDER BY credit_limit NULLS FIRST)
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.order_status
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.status AS order_status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(running_age) OVER w_grp AS next_running_age,
        LAG(credit_limit_dense_rank) OVER w_grp AS prev_credit_limit_dense_rank,
        ROW_NUMBER() OVER w_grp AS seq_no
    FROM with_last
    WINDOW w_grp AS (PARTITION BY city ORDER BY credit_limit, id)
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_credit_limit_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS prev_credit_limit_dense_rank_tier,
    DATE_TRUNC('MONTH', signup_date) AS activity_month,
    PERCENTILE_APPROX(running_age, ARRAY(0.25, 0.5, 0.75)) AS running_age_quartiles,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM scored
GROUP BY CASE WHEN prev_credit_limit_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', signup_date)
ORDER BY activity_month;
