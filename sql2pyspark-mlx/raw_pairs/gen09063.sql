WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        store_id,
        amount,
        RANK() OVER (PARTITION BY YEAR(order_date) ORDER BY discount DESC) AS discount_rank,
        FIRST_VALUE(amount, TRUE) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM orders
    WHERE order_date > '2024-09-14'
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id, store_id ORDER BY discount_rank NULLS FIRST, id) AS rank_in_group
    FROM with_windows
    QUALIFY rank_in_group = 1
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.discount_rank,
        s.first_known_amount,
        s.rank_in_group,
        p.sqft
    FROM latest s
    JOIN stores p
        ON s.store_id <=> p.id
),
with_last AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.discount_rank,
        s.first_known_amount,
        s.rank_in_group,
        s.sqft,
        top1.amount,
        top1.status
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.rank_in_group,
        e.sqft,
        e.amount,
        e.status,
        s.sqft AS store_sqft,
        s.city,
        s.opened_date
    FROM with_last e
    LEFT JOIN stores s
        ON e.store_id = s.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount >= 1370 THEN 'HIGH' ELSE 'LOW' END AS amount_tier,
    APPROX_PERCENTILE(store_sqft, 0.5) AS store_sqft_p50,
    ROUND(AVG(sqft), 2) AS avg_sqft,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list,
    COUNT(CASE WHEN city = 'Tokyo' THEN 1 END) AS tokyo_count,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    SIZE(COLLECT_SET(status)) AS num_status
FROM joined
GROUP BY CASE WHEN amount >= 1370 THEN 'HIGH' ELSE 'LOW' END
