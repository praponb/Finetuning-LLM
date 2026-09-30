WITH joined AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.quantity,
        e.last_restock,
        b.opened_date,
        b.region
    FROM inventory e
    LEFT JOIN stores b
        ON e.store_id = b.id
    WHERE b.sqft != 13534
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY store_id, region ORDER BY last_restock ASC NULLS LAST) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        quantity,
        opened_date,
        region,
        dedup_rank,
        NTILE(3) OVER (PARTITION BY region ORDER BY quantity DESC, id ASC) AS quantity_bucket,
        NTILE(4) OVER (PARTITION BY region ORDER BY quantity DESC, id ASC) AS quantity_bucket_2
    FROM latest
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY region ORDER BY quantity_bucket_2 ASC, id ASC) AS quantity_bucket_2_bucket,
        PERCENT_RANK() OVER w_running AS quantity_percent_rank
    FROM with_windows
    WINDOW w_running AS (PARTITION BY store_id ORDER BY quantity DESC NULLS LAST)
),
with_dims AS (
    -- Attach stores attributes
    SELECT e.*, p.sqft
    FROM enriched e
    LEFT JOIN stores p
        ON e.store_id = p.id
),
joined_inventory AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.quantity,
        src.opened_date,
        src.dedup_rank,
        src.quantity_bucket_2_bucket,
        src.quantity_percent_rank,
        s.region AS store_region
    FROM with_dims src
    JOIN stores s
        ON src.store_id = s.id
)
SELECT
    s.id,
    s.store_id,
    s.opened_date,
    s.quantity_percent_rank,
    s.store_region,
    CASE WHEN store_region RLIKE '^(North|Central)$' THEN 'OTHER_ZONE' WHEN store_region RLIKE '^(South|East)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS store_region_zone,
    (SELECT SUM(t2.discount) FROM orders t2 WHERE t2.store_id = s.store_id) AS sum_order_discount
FROM joined_inventory s
ORDER BY store_region_zone DESC NULLS FIRST
