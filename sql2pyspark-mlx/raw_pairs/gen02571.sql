WITH metrics AS (
    SELECT
        *,
        SUM(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_sum_quantity,
        AVG(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_avg_quantity,
        COUNT(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_count_quantity,
        MIN(quantity) OVER (PARTITION BY store_id ORDER BY last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) last7_min_quantity,
        CUME_DIST() OVER (PARTITION BY store_id ORDER BY quantity NULLS FIRST) quantity_cume_dist
    FROM inventory
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY last7_avg_quantity DESC, id) AS rn
    FROM metrics
    QUALIFY rn = 1
),
enriched AS (
    SELECT
        a.*,
        s.sqft,
        s.city
    FROM ranked a
    LEFT JOIN stores s
        ON a.store_id = s.id
    WHERE s.sqft IS NOT NULL
),
joined AS (
    SELECT
        s.*,
        b.region,
        b.store_name
    FROM enriched s
    INNER JOIN stores b
        ON s.store_id = b.id
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.quantity,
    s.last_restock,
    s.last7_sum_quantity,
    s.last7_count_quantity,
    s.rn,
    s.sqft,
    (SELECT MIN(t2.amount) FROM orders t2 WHERE t2.store_id = s.store_id) AS min_order_amount
FROM joined s
ORDER BY min_order_amount DESC NULLS LAST
