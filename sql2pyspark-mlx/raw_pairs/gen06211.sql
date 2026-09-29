WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city, country ORDER BY rating DESC NULLS FIRST) AS rn
        FROM suppliers
    ) x
    WHERE rn = 1
)
SELECT
    id,
    supplier_name,
    country,
    rating,
    rn,
    CASE WHEN supplier_name RLIKE '^(Tokyo Traders|Pacific Wholesale)$' THEN 'GROUP_A' WHEN supplier_name RLIKE '^(Siam Parts Co|Global Supply)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS supplier_name_zone
FROM deduped
ORDER BY supplier_name_zone DESC NULLS FIRST;
