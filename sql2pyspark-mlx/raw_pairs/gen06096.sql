WITH matched_keys AS (
    -- Set intersection on keys
    SELECT id, store_id
    FROM inventory
    INTERSECT ALL
    SELECT id, store_id
    FROM inventory
    WHERE last_restock > '2024-01-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM inventory s
    INNER JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        quantity,
        last_restock,
        PERCENT_RANK() OVER (PARTITION BY YEAR(last_restock), product_id ORDER BY quantity) AS quantity_percent_rank,
        MAX(quantity) OVER (PARTITION BY DATE_TRUNC('YEAR', last_restock), product_id) AS last_restock_period_max_quantity,
        COUNT(quantity) OVER w_running AS store_id_count_quantity,
        STDDEV_SAMP(quantity) OVER w_running AS store_id_stddev_samp_quantity,
        CUME_DIST() OVER (PARTITION BY product_id ORDER BY quantity DESC NULLS FIRST) AS quantity_cume_dist
    FROM reconciled
    WINDOW w_running AS (PARTITION BY store_id)
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.sqft,
        p2.city,
        p2.store_name
    FROM enriched s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
    WHERE p2.store_name IS NOT NULL
),
enriched_inventory AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.store_name AS store_store_name,
        b.opened_date,
        b.sqft AS store_sqft
    FROM with_dims a
    INNER JOIN stores b
        ON a.store_id = b.id
),
enriched_inventory_2 AS (
    -- Attach stores attributes
    SELECT
        a.*,
        s.opened_date AS store_opened_date,
        s.sqft AS store_sqft_2,
        s.city AS store_city
    FROM enriched_inventory a
    LEFT JOIN stores s
        ON a.store_id = s.id
)
-- Join a dimension and aggregate
SELECT
    d.city AS store_city,
    d.region,
    COUNT(*) AS num_rows,
    ROUND(SUM(s.store_sqft_2), 2) AS total_store_sqft_2,
    GROUPING_ID() AS gid
FROM enriched_inventory_2 s
INNER JOIN stores d ON s.store_id = d.id
GROUP BY ROLLUP (d.city, d.region);
