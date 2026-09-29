WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        MONTH(last_restock) AS last_restock_month,
        DATE_TRUNC('month', last_restock) AS last_restock_period
    FROM inventory
    WHERE product_id IN (3, 4, 8)
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(last_restock_period) OVER (PARTITION BY store_id ORDER BY last_restock_period, id) IS NULL
                 OR last_restock_period > LAG(last_restock_period) OVER (PARTITION BY store_id ORDER BY last_restock_period, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY store_id ORDER BY last_restock_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
with_dims AS (
    -- Attach products attributes
    SELECT e.*, d.brand
    FROM islands e
    JOIN products d
        ON e.product_id = d.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        last_restock_month,
        session_start_flag,
        island_id,
        brand,
        LEAD(island_id, 1) OVER (PARTITION BY brand ORDER BY session_start_flag, id) AS next_island_id,
        ROW_NUMBER() OVER w_part AS seq_no,
        SUM(island_id) OVER (PARTITION BY MONTH(last_restock), store_id ORDER BY quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_island_id
    FROM with_dims
    WINDOW w_part AS (PARTITION BY product_id ORDER BY quantity, id)
),
joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.last_restock,
        e.session_start_flag,
        e.next_island_id,
        e.seq_no,
        e.running_island_id,
        b.region,
        b.city,
        b.store_name
    FROM enriched e
    LEFT JOIN stores b
        ON e.store_id = b.id
    WHERE YEAR(b.opened_date) >= 2025
),
with_dims_inventory AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.region AS store_region,
        b.city AS store_city
    FROM joined a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
SELECT
    id,
    last_restock,
    session_start_flag,
    next_island_id,
    seq_no,
    region,
    city,
    store_region
FROM with_dims_inventory
ORDER BY city, store_region
