WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        e.id,
        e.store_id,
        e.last_restock,
        d.city,
        d.sqft
    FROM inventory e
    LEFT JOIN stores d
        ON e.store_id = d.id
    WHERE d.city = 'Singapore'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        last_restock,
        MAX(sqft) OVER (PARTITION BY store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_sqft,
        COUNT(sqft) OVER (PARTITION BY store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_sqft,
        MIN(sqft) OVER (PARTITION BY store_id ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_sqft,
        SUM(sqft) OVER (PARTITION BY city ORDER BY last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM with_dims
),
remaining_keys AS (
    -- Set difference on keys
    SELECT store_id, id
    FROM with_windows
    EXCEPT
    SELECT store_id, id
    FROM inventory
    WHERE product_id IN (1, 3)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
with_windows_inventory AS (
    -- Running and moving aggregates
    SELECT
        id,
        last_restock,
        running_max_sqft,
        ROUND(running_count_sqft / SUM(running_count_sqft) OVER (PARTITION BY store_id), 4) AS running_count_sqft_share,
        ROUND(cumulative_sqft / SUM(cumulative_sqft) OVER (PARTITION BY store_id), 4) AS cumulative_sqft_share,
        ROUND(running_min_sqft / SUM(running_min_sqft) OVER (PARTITION BY store_id), 4) AS running_min_sqft_share
    FROM kept_rows
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows_inventory
    EXCEPT ALL
    SELECT id
    FROM inventory
    WHERE product_id IN (1, 7, 8)
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_inventory s
    JOIN kept_keys k ON s.id = k.id
)
-- Running and moving aggregates
SELECT
    running_max_sqft,
    cumulative_sqft_share,
    RANK() OVER (ORDER BY cumulative_sqft_share DESC) AS cumulative_sqft_share_rank
FROM reconciled
