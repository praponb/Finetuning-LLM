WITH cleaned AS (
    SELECT
        *,
        CASE WHEN price >= 960 THEN 'High' WHEN price >= 160 THEN 'Medium' ELSE 'Low' END AS price_band,
        DATE_TRUNC('WEEK', launch_date) AS launch_date_period,
        COALESCE(brand, 'NONE') AS resolved_brand
    FROM products
    WHERE cost < 650
)
SELECT *
FROM (
    SELECT
        id,
        brand,
        price,
        cost
    FROM cleaned
) src
UNPIVOT (
    reading FOR measure IN (price, cost)
)
