WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) AS age_share,
        ROW_NUMBER() OVER (ORDER BY age, id) AS seq_no,
        CUME_DIST() OVER w_ordered AS age_cume_dist,
        LEAD(age, 1) OVER (PARTITION BY country, segment ORDER BY credit_limit, id) AS next_age,
        FIRST_VALUE(age) IGNORE NULLS OVER (PARTITION BY city, country ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_age
    FROM customers
    WHERE city IN ('Tokyo', 'Chiang Mai', 'London')
    WINDOW w_ordered AS (PARTITION BY country, segment ORDER BY age DESC)
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY segment, city ORDER BY signup_date) AS latest_rank
        FROM scored
    ) t
    WHERE latest_rank = 1
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.city,
        s.credit_limit,
        s.age,
        s.seq_no,
        s.next_age,
        s.first_known_age,
        latest.order_order_date,
        latest.shipping_fee
    FROM latest s
    JOIN LATERAL (
            SELECT c.order_date AS order_order_date, c.shipping_fee
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored_customers AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        age,
        next_age,
        shipping_fee,
        SUM(credit_limit) OVER (PARTITION BY city ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_credit_limit,
        MIN(credit_limit) OVER (PARTITION BY city ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_credit_limit,
        AVG(credit_limit) OVER (PARTITION BY city ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_credit_limit,
        MAX(credit_limit) OVER (PARTITION BY city ORDER BY shipping_fee, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_credit_limit
    FROM with_last
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored_customers s
    WHERE EXISTS (
            SELECT 1
            FROM sessions c
            WHERE c.customer_id = s.id
              AND TO_DATE(c.started_at) < '2023-08-23'
        )
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN shipping_fee >= 31 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(shipping_fee, ARRAY(0.25, 0.5, 0.75)) AS shipping_fee_quartiles,
    APPROX_PERCENTILE(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles
FROM kept
GROUP BY shipping_fee_tier;
