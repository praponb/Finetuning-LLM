WITH filtered AS (
    SELECT
        *,
        SIZE(tags) AS tags_count,
        COALESCE(brand, 'DIRECT') AS brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
),
with_last AS (
    SELECT
        s.id,
        s.category,
        s.price,
        s.supplier_id,
        s.brand_filled,
        latest.last_restock,
        latest.inventory_quantity
    FROM filtered s
    JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY inventory_quantity DESC, id) AS row_num
    FROM with_last
    QUALIFY row_num = 1
),
scored AS (
    SELECT
        id,
        supplier_id,
        row_num,
        LEAD(inventory_quantity, 2) OVER (PARTITION BY category ORDER BY row_num, id) AS next_inventory_quantity,
        MIN(price) OVER w_part AS supplier_id_min_price,
        MIN(inventory_quantity) OVER w_part AS supplier_id_min_inventory_quantity
    FROM deduped
    WHERE supplier_id IS NULL
    WINDOW w_part AS (PARTITION BY supplier_id)
),
enriched AS (
    SELECT
        id,
        supplier_id,
        row_num,
        supplier_id_min_price,
        AVG(next_inventory_quantity) OVER (PARTITION BY supplier_id ORDER BY supplier_id_min_price, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS next_inventory_quantity_avg_5_prior,
        PERCENT_RANK() OVER (PARTITION BY supplier_id ORDER BY supplier_id_min_inventory_quantity DESC NULLS FIRST) AS supplier_id_min_inventory_quantity_percent_rank,
        SUM(supplier_id_min_inventory_quantity) OVER (PARTITION BY supplier_id ORDER BY next_inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_supplier_id_min_inventory_quantity
    FROM scored
),
metrics AS (
    SELECT
        id,
        row_num,
        supplier_id_min_price,
        next_inventory_quantity_avg_5_prior,
        supplier_id_min_inventory_quantity_percent_rank,
        running_supplier_id_min_inventory_quantity,
        MAX(running_supplier_id_min_inventory_quantity) OVER w_recent AS running_max_running_supplier_id_min_inventory_quantity,
        COUNT(running_supplier_id_min_inventory_quantity) OVER w_recent AS running_count_running_supplier_id_min_inventory_quantity,
        SUM(running_supplier_id_min_inventory_quantity) OVER w_recent AS running_sum_running_supplier_id_min_inventory_quantity
    FROM enriched
    WINDOW w_recent AS (PARTITION BY supplier_id ORDER BY next_inventory_quantity_avg_5_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN supplier_id_min_price >= 1191 THEN 'PREMIUM' ELSE 'STANDARD' END AS supplier_id_min_price_tier,
    APPROX_PERCENTILE(supplier_id_min_price, 0.9) AS supplier_id_min_price_p90,
    COUNT(DISTINCT id) AS distinct_ids
FROM metrics
GROUP BY CASE WHEN supplier_id_min_price >= 1191 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(DISTINCT id) < 1000
ORDER BY supplier_id_min_price_p90 DESC
