WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date
    FROM store_sales
    WHERE sale_date >= DATE '2023-07-23'
      AND sale_date < '2023-09-23'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT id, store_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC, id) AS latest_rank
        FROM cleaned
    ) ranked
    WHERE latest_rank = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.*,
        b.opened_date,
        b.sqft,
        b.store_name
    FROM ranked e
    LEFT JOIN stores b
        ON e.store_id = b.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM joined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY opened_date DESC NULLS LAST, id) = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM deduped
    EXCEPT
    SELECT id, store_id
    FROM store_sales
    WHERE units BETWEEN 16 AND 24
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
)
-- Join a dimension and aggregate
SELECT
    dim.region,
    dim.city,
    COUNT(*) AS num_rows,
    SUM(src.sqft) AS total_sqft,
    GROUPING_ID() AS level_id
FROM reconciled src
LEFT JOIN stores dim ON src.store_id = dim.id
GROUP BY CUBE (dim.region, dim.city)
ORDER BY city NULLS FIRST
