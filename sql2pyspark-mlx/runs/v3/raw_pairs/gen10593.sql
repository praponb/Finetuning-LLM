WITH FilteredInventory AS (
    SELECT *
    FROM inventory
    WHERE quantity <> 107
),
QuantityBucketSummary AS (
    SELECT CASE WHEN quantity >= 190 THEN 'high' WHEN quantity >= 64 THEN 'mid' ELSE 'low' END AS quantity_bucket, SUM(quantity) AS total_quantity
    FROM FilteredInventory
    GROUP BY CASE WHEN quantity >= 190 THEN 'high' WHEN quantity >= 64 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM QuantityBucketSummary
