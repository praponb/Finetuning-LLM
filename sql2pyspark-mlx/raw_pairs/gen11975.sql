WITH filtered AS (
    SELECT
        id,
        store_id,
        sale_date,
        units,
        revenue,
        CAST(units AS DOUBLE) AS units_str
    FROM store_sales
    WHERE sale_date > DATE '2023-06-13'
      AND store_id = 6
)
SELECT
    a.*,
    d.sqft,
    d.store_name,
    d.region
FROM filtered a
INNER JOIN stores d
    ON a.store_id = d.id;
