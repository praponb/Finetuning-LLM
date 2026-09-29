WITH filtered AS (
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        REGEXP_REPLACE(store_name, ' ', '-') AS store_name_slug,
        INITCAP(TRIM(city)) AS city_title
    FROM stores
    WHERE opened_date >= DATE '2023-05-22'
      AND region IS NOT NULL
)
SELECT
    id,
    city,
    region,
    store_name_slug,
    city_title
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) AS rn
    FROM filtered
) x
WHERE rn = 1;
