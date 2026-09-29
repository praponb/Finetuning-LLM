SELECT capacity AS ref_id, city
FROM warehouses
EXCEPT
SELECT id AS ref_id, category AS city
FROM skus
WHERE LENGTH(TRIM(sku_code)) >= 4
ORDER BY city DESC
