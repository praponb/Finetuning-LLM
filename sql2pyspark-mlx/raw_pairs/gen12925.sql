WITH kept_keys AS (
    SELECT product_id, id
    FROM inventory
    EXCEPT
    SELECT product_id, id
    FROM inventory
    WHERE product_id = 2
),
reconciled AS (
    SELECT s.*
    FROM inventory s
    INNER JOIN kept_keys k ON s.product_id = k.product_id AND s.id = k.id
)
SELECT
    CASE WHEN quantity >= 204 THEN 'HIGH' ELSE 'LOW' END AS quantity_tier,
    COUNT(DISTINCT product_id) AS distinct_product_ids,
    COUNT(*) AS cnt,
    MAX(quantity) AS max_quantity,
    STDDEV_SAMP(quantity) AS quantity_stddev,
    BIT_OR(quantity) AS quantity_bit_or,
    SUM(quantity) AS total_quantity
FROM reconciled
GROUP BY CASE WHEN quantity >= 204 THEN 'HIGH' ELSE 'LOW' END
ORDER BY max_quantity DESC NULLS LAST
