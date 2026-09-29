WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        d.opened_date,
        d.city,
        d.region
    FROM inventory src
    LEFT JOIN stores d
        ON src.store_id = d.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATEDIFF(last_restock, opened_date) AS days_opened_date_to_last_restock,
        COALESCE(city, 'UNKNOWN') AS city_filled
    FROM with_dims
    WHERE region IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        quantity,
        last_restock,
        opened_date,
        city,
        region,
        days_opened_date_to_last_restock,
        city_filled,
        MIN(quantity) OVER (PARTITION BY product_id, region) AS product_id_min_quantity,
        STDDEV_SAMP(quantity) OVER w_part AS city_stddev_samp_quantity,
        LAG(quantity, 2, 0) OVER (PARTITION BY store_id ORDER BY last_restock, id) AS prev_quantity,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM base
    WINDOW w_part AS (PARTITION BY city), w_recent AS (PARTITION BY region ORDER BY quantity, id)
),
joined AS (
    -- Attach products attributes
    SELECT
        s.*,
        p.price,
        p.cost
    FROM metrics s
    LEFT JOIN products p
        ON s.product_id = p.id
),
kept_keys AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM joined
    EXCEPT DISTINCT
    SELECT product_id, store_id
    FROM inventory
    WHERE product_id IN (2, 4, 5, 10)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM joined s
    JOIN kept_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
metrics_inventory AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY region), 4) AS event_seq_share,
        CUME_DIST() OVER (PARTITION BY city ORDER BY city_stddev_samp_quantity DESC NULLS FIRST) AS city_stddev_samp_quantity_cume_dist
    FROM reconciled
),
joined_inventory AS (
    -- Attach products attributes
    SELECT
        a.*,
        d.category,
        d.stock_qty
    FROM metrics_inventory a
    LEFT JOIN products d
        ON a.product_id = d.id
)
SELECT
    id,
    store_id,
    product_id,
    last_restock,
    opened_date,
    city,
    region,
    days_opened_date_to_last_restock,
    city_filled,
    CASE WHEN region RLIKE '^(Central|West)$' THEN 'PRIORITY' WHEN region RLIKE '^(North|South)$' THEN 'CORE' ELSE 'GROUP_B' END AS region_zone
FROM joined_inventory
ORDER BY store_id;
