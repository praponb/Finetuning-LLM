WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(rating, 2) rating_rounded
    FROM suppliers
    WHERE city IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    id,
    city,
    measure,
    metric_value
FROM (
    SELECT
        id,
        city,
        rating_rounded,
        rating
    FROM cleaned
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR measure IN (rating_rounded AS RATING_ROUNDED_VAL, rating AS RATING_VAL)
) unp;
