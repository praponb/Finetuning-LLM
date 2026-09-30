WITH cleaned AS (
    -- Scope the warehouses rows
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on,
        CONCAT(wh_code, '#', CAST(id AS STRING)) AS wh_code_ref
    FROM warehouses
    WHERE UPPER(TRIM(wh_code)) LIKE 'B%'
      AND opened_on < DATE '2025-02-14'
)
SELECT id
FROM cleaned
WHERE capacity >= 2614
  AND wh_code_ref IS NOT NULL
EXCEPT
SELECT id
FROM skus
