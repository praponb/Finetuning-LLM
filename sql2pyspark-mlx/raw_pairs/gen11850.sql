WITH valid AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        FLOOR(units) AS units_whole
    FROM store_sales
    WHERE sale_date >= '2024-02-01'
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY product_id, store_id ORDER BY sale_date DESC) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
enriched AS (
    SELECT
        e.id,
        e.store_id,
        e.sale_date,
        e.units,
        e.units_whole,
        e.latest_rank,
        b.sqft,
        b.region,
        b.store_name
    FROM ranked e
    LEFT JOIN stores b
        ON e.store_id = b.id
    WHERE b.sqft >= 7564
),
cleaned AS (
    SELECT
        id,
        sale_date,
        units,
        units_whole,
        sqft,
        region,
        DAYOFWEEK(sale_date) AS sale_date_dayofweek
    FROM enriched
    WHERE region IS NOT NULL
      AND region = 'Central'
)
SELECT
    id,
    units,
    sqft,
    sale_date_dayofweek
FROM cleaned
ORDER BY sale_date_dayofweek DESC;
