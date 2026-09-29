WITH valid AS (
    SELECT
        *,
        DATE_TRUNC('MONTH', sale_date) AS sale_date_period,
        CAST(units AS DOUBLE) AS units_str
    FROM store_sales
    WHERE sale_date > DATE '2023-04-13'
),
joined AS (
    SELECT
        s.id,
        s.store_id,
        s.product_id,
        s.sale_date,
        s.units,
        s.revenue,
        s.units_str,
        p.region
    FROM valid s
    LEFT JOIN stores p
        ON s.store_id = p.id
    WHERE p.city IN ('Bangkok', 'New York')
),
base AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        units_str,
        region,
        CAST(units_str AS INT) AS units_str_int,
        COALESCE(region, 'N/A') AS region_filled
    FROM joined
    WHERE sale_date >= DATE '2023-08-24'
      AND region IS NOT NULL
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units_str DESC, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
)
SELECT *
FROM (
    SELECT
        store_id,
        region,
        revenue
    FROM deduped
) src
PIVOT (
    AVG(revenue) AS total
    FOR region IN ('South', 'East', 'West')
)
