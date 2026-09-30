WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(store_name) AS clean_store_name
    FROM stores
    WHERE opened_date >= TO_DATE('2023-04-21')
      AND region IS NOT NULL
)
-- Semi-join via a correlated subquery
SELECT s.*
FROM base s
WHERE EXISTS (
        SELECT 1
        FROM store_sales c
        WHERE c.store_id = s.id
          AND c.sale_date <= s.opened_date
    );
