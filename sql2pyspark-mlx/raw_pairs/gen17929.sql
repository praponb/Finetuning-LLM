WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365.25 AS years_since_last_restock
    FROM inventory
    WHERE last_restock > TO_DATE('2023-06-17')
      AND last_restock <= '2024-08-27'
),
joined AS (
    -- Attach stores attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.quantity,
        a.last_restock,
        a.years_since_last_restock,
        b.store_name
    FROM base a
    LEFT JOIN stores b
        ON a.store_id = b.id
),
joined_inventory AS (
    -- Attach products attributes
    SELECT
        a.*,
        p.category,
        p.launch_date,
        p.price
    FROM joined a
    LEFT JOIN products p
        ON a.product_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        years_since_last_restock,
        store_name
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY category, product_id ORDER BY launch_date DESC, id) AS latest_rank
        FROM joined_inventory
    ) t
    WHERE latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        years_since_last_restock,
        LAG(years_since_last_restock) OVER w_running AS prev_years_since_last_restock,
        ROW_NUMBER() OVER w_running AS row_seq,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY years_since_last_restock DESC, id) AS years_since_last_restock_bucket
    FROM deduped
    WINDOW w_running AS (PARTITION BY product_id ORDER BY years_since_last_restock, id)
)
-- Join a dimension and aggregate
SELECT
    dim.region,
    dim.city,
    COUNT(*) AS num_rows,
    MAX(a.row_seq) AS max_row_seq,
    GROUPING_ID() AS gid
FROM with_windows a
LEFT JOIN stores dim ON a.store_id = dim.id
GROUP BY GROUPING SETS ((dim.region, dim.city), (dim.region), ());
