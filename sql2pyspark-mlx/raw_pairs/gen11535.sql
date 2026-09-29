WITH filtered AS (
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        CASE WHEN quantity >= 254 THEN 'High' WHEN quantity >= 41 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE last_restock >= DATE '2023-03-10'
),
enriched AS (
    SELECT
        e.*,
        d.city,
        d.region
    FROM filtered e
    JOIN stores d
        ON e.store_id = d.id
    WHERE d.sqft <= 14546
),
metrics AS (
    SELECT
        *,
        MAX(quantity) OVER (PARTITION BY city ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_quantity,
        COUNT(quantity) OVER (PARTITION BY city ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_quantity,
        AVG(quantity) OVER (PARTITION BY city ORDER BY quantity, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_quantity,
        NTILE(3) OVER (PARTITION BY store_id ORDER BY quantity, id) AS quantity_bucket
    FROM enriched
),
scored AS (
    SELECT
        id,
        store_id,
        quantity,
        city,
        region,
        last4_max_quantity,
        last4_avg_quantity,
        MAX(last4_avg_quantity) OVER w_ordered AS last3_max_last4_avg_quantity,
        MIN(last4_avg_quantity) OVER w_ordered AS last3_min_last4_avg_quantity,
        COUNT(last4_avg_quantity) OVER w_ordered AS last3_count_last4_avg_quantity
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY region, store_id ORDER BY last4_max_quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.store_id,
    s.quantity,
    s.region,
    s.last4_max_quantity,
    s.last3_max_last4_avg_quantity,
    s.last3_min_last4_avg_quantity,
    s.last3_count_last4_avg_quantity,
    (SELECT SUM(t2.units) FROM store_sales t2 WHERE t2.store_id = s.store_id) AS sum_store_sale_units
FROM scored s
ORDER BY last3_max_last4_avg_quantity, last4_max_quantity DESC;
