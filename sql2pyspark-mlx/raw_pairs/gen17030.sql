WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units <= 26
        )
)
SELECT
    id,
    city,
    region,
    opened_date
FROM not_flagged;
