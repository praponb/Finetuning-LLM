WITH latest AS (
    SELECT *, RANK() OVER (PARTITION BY store_id, product_id ORDER BY last_restock DESC) AS rn
    FROM inventory
    QUALIFY rn = 1
),
joined AS (
    SELECT e.*, s.sqft
    FROM latest e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
remaining_keys AS (
    SELECT store_id, product_id
    FROM joined
    INTERSECT ALL
    SELECT store_id, product_id
    FROM inventory
    WHERE store_id IN (2, 4, 6)
),
reconciled AS (
    SELECT s.*
    FROM joined s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
),
metrics AS (
    SELECT
        *,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY product_id), 4) AS sqft_share,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY sqft NULLS FIRST) AS sqft_dense_rank,
        AVG(sqft) OVER (PARTITION BY product_id, store_id) AS product_id_avg_sqft,
        STDDEV_SAMP(sqft) OVER (PARTITION BY store_id) AS store_id_stddev_samp_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY store_id), 4) AS sqft_share_2
    FROM reconciled
),
scored AS (
    SELECT
        *,
        MIN(product_id_avg_sqft) OVER (PARTITION BY product_id) AS product_id_min_product_id_avg_sqft,
        RANK() OVER (ORDER BY sqft_share DESC) AS sqft_share_rank
    FROM metrics
),
with_dims AS (
    SELECT e.*, d.sqft AS store_sqft
    FROM scored e
    LEFT JOIN stores d
        ON e.store_id = d.id
),
enriched AS (
    SELECT a.*, s.store_name
    FROM with_dims a
    INNER JOIN stores s
        ON a.store_id = s.id
),
stats AS (
    SELECT
        product_id,
        CASE WHEN product_id_min_product_id_avg_sqft >= 16614 THEN 'LARGE' ELSE 'SMALL' END AS product_id_min_product_id_avg_sqft_tier,
        store_id,
        COUNT(DISTINCT product_id) AS distinct_product_ids,
        MAX(store_sqft) AS max_store_sqft
    FROM enriched
    GROUP BY product_id, CASE WHEN product_id_min_product_id_avg_sqft >= 16614 THEN 'LARGE' ELSE 'SMALL' END, store_id
)
SELECT
    s.product_id,
    s.product_id_min_product_id_avg_sqft_tier,
    s.store_id,
    s.max_store_sqft,
    (SELECT MIN(o2.unit_price) FROM order_items o2 WHERE o2.product_id = s.product_id) AS min_order_item_unit_price
FROM stats s
