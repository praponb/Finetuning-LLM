WITH valid AS (
    SELECT *, DATE_ADD(last_restock, 78) AS last_restock_plus_78d
    FROM inventory
    WHERE last_restock >= DATE '2023-08-29'
      AND store_id = 4
)
SELECT
    CASE WHEN quantity >= 227 THEN 'LARGE' ELSE 'SMALL' END AS quantity_tier,
    product_id,
    store_id,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    COUNT(*) AS row_count,
    SUM(quantity) AS total_quantity,
    GROUPING(CASE WHEN quantity >= 227 THEN 'LARGE' ELSE 'SMALL' END) AS gid
FROM valid
GROUP BY GROUPING SETS ((CASE WHEN quantity >= 227 THEN 'LARGE' ELSE 'SMALL' END, product_id, store_id), (CASE WHEN quantity >= 227 THEN 'LARGE' ELSE 'SMALL' END, product_id), ())
HAVING row_count < 1000;
