WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        LEAD(sqft, 2) OVER (ORDER BY opened_date ASC, id ASC) AS next_sqft,
        NTILE(3) OVER w_hist AS sqft_bucket
    FROM stores
    WINDOW w_hist AS (PARTITION BY region ORDER BY sqft DESC, id ASC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(3) OVER w_recent AS next_sqft_bucket,
        ROW_NUMBER() OVER w_recent AS event_seq,
        NTILE(10) OVER (ORDER BY sqft_bucket DESC, id ASC) AS sqft_bucket_bucket
    FROM metrics
    WINDOW w_recent AS (ORDER BY next_sqft ASC, id ASC)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        SUBSTRING(store_name, 1, 2) AS store_name_prefix,
        ROUND(next_sqft_bucket, 2) AS next_sqft_bucket_rounded
    FROM with_windows
    WHERE sqft_bucket_bucket = 6
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.channel = 'web'
        )
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.status IN ('SHIPPED', 'PENDING', 'COMPLETED')
        )
),
with_latest AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.last_restock,
        latest.inventory_quantity
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders c
            WHERE c.store_id = s.id
              AND c.amount >= 780
        )
)
-- Ranking and gap features
SELECT
    id,
    store_name,
    next_sqft,
    sqft_bucket,
    store_name_prefix,
    inventory_quantity,
    AVG(event_seq) OVER (PARTITION BY DATE_TRUNC('WEEK', last_restock) ORDER BY sqft_bucket ASC, id ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS event_seq_avg_3,
    SUM(sqft_bucket_bucket) OVER (ORDER BY next_sqft_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft_bucket_bucket,
    LAG(inventory_quantity, 1) OVER (ORDER BY sqft_bucket_bucket ASC, id ASC) AS prev_inventory_quantity,
    PERCENT_RANK() OVER (ORDER BY inventory_quantity ASC NULLS FIRST) AS inventory_quantity_percent_rank,
    NTILE(10) OVER (ORDER BY inventory_quantity ASC, id ASC) AS inventory_quantity_bucket
FROM not_flagged_stores
WHERE sqft_bucket IS NULL
