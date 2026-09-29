WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        city,
        CONCAT_WS(',', supplier_name, country) AS supplier_name_country_key
    FROM suppliers
    WHERE city IS NOT NULL
      AND city = 'Bangkok'
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND MONTH(ch.launch_date) IN (1, 4, 9)
        )
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.brand IN ('Acme', 'Initech', 'Umbrella')
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN city = 'Tokyo' THEN 1 ELSE 0 END AS is_tokyo,
        TRIM(city) AS clean_city
    FROM not_flagged
    WHERE city IS NOT NULL
      AND UPPER(supplier_name) LIKE '%IAM%'
)
SELECT
    id,
    supplier_name,
    supplier_name_country_key,
    is_tokyo,
    clean_city,
    CASE WHEN supplier_name RLIKE '^(Lion City Imports|EuroGoods GmbH)$' THEN 'GROUP_A' WHEN supplier_name RLIKE '^(Siam Parts Co|Global Supply)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS supplier_name_zone
FROM cleaned
ORDER BY supplier_name_zone NULLS LAST, is_tokyo
