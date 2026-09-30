WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(quantity, 0) quantity_filled
    FROM inventory
    WHERE last_restock > '2025-02-23'
),
filtered_inventory AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        quantity + quantity_filled quantity_plus_quantity_filled,
        DATEDIFF(CURRENT_DATE(), last_restock) / 365 years_since_last_restock
    FROM filtered
    WHERE store_id IN (1, 4, 5, 6)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        last_restock,
        quantity_filled,
        quantity_plus_quantity_filled,
        AVG(years_since_last_restock) OVER w_hist years_since_last_restock_avg_6,
        DENSE_RANK() OVER (PARTITION BY product_id, store_id ORDER BY years_since_last_restock DESC NULLS LAST) years_since_last_restock_dense_rank
    FROM filtered_inventory
    WHERE MONTH(last_restock) IN (1, 5, 9)
    WINDOW w_hist AS (ORDER BY years_since_last_restock, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT store_id, id
    FROM with_windows
    INTERSECT
    SELECT store_id, id
    FROM inventory
    WHERE product_id IN (4, 8)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.store_id = k.store_id AND s.id = k.id
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM kept_rows
    EXCEPT DISTINCT
    SELECT id, store_id
    FROM inventory
    WHERE product_id = 5
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN matched_keys k ON s.id = k.id AND s.store_id = k.store_id
)
-- Join a dimension and aggregate
SELECT
    p.region,
    COUNT(*) AS cnt,
    ROUND(SUM(e.years_since_last_restock_dense_rank), 2) AS total_years_since_last_restock_dense_rank,
    ROUND(SUM(e.quantity_plus_quantity_filled), 2) AS total_quantity_plus_quantity_filled
FROM reconciled e
INNER JOIN stores p ON e.store_id = p.id
GROUP BY p.region
