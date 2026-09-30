WITH matched_keys AS (
    SELECT store_id, product_id
    FROM inventory
    EXCEPT ALL
    SELECT store_id, product_id
    FROM inventory
    WHERE quantity IS NOT NULL
),
verified AS (
    SELECT s.*
    FROM inventory s
    INNER JOIN matched_keys k ON s.store_id = k.store_id AND s.product_id = k.product_id
)
SELECT
    store_id,
    APPROX_PERCENTILE(quantity, ARRAY(0.25, 0.5, 0.75)) AS quantity_quartiles,
    STDDEV_SAMP(quantity) AS quantity_stddev
FROM verified
GROUP BY store_id
HAVING STDDEV_SAMP(quantity) >= 0;
