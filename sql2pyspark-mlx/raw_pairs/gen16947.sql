WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        last_restock,
        CASE WHEN quantity >= 256 THEN 'High' WHEN quantity >= 86 THEN 'Medium' ELSE 'Low' END AS quantity_tier
    FROM inventory
    WHERE product_id IN (5, 7, 9)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock DESC, id) AS event_seq
    FROM valid
    WHERE product_id IN (3, 5, 8)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.*,
        d.opened_date,
        d.city,
        d.sqft
    FROM with_windows a
    JOIN stores d
        ON a.store_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(event_seq) OVER (PARTITION BY city ORDER BY event_seq, id) AS prev_event_seq,
        LEAD(sqft) OVER (PARTITION BY city ORDER BY event_seq, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY event_seq, id) AS row_seq,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY product_id, store_id), 4) AS sqft_share
    FROM enriched
),
with_windows_inventory AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(sqft, 1, 0) OVER (ORDER BY sqft, id) AS next_sqft_2,
        LAG(sqft, 2, 0) OVER (PARTITION BY YEAR(last_restock), city ORDER BY opened_date, id) AS prev_sqft,
        NTILE(10) OVER (PARTITION BY city ORDER BY event_seq, id) AS event_seq_bucket,
        FIRST_VALUE(next_sqft) IGNORE NULLS OVER w_hist AS first_known_next_sqft
    FROM metrics
    WINDOW w_hist AS (PARTITION BY product_id ORDER BY prev_event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Join a dimension and aggregate
SELECT
    p.brand,
    a.city,
    COUNT(*) AS num_rows,
    AVG(a.next_sqft) AS avg_next_sqft,
    ROUND(SUM(a.prev_event_seq), 2) AS total_prev_event_seq
FROM with_windows_inventory a
JOIN products p ON a.product_id = p.id
GROUP BY GROUPING SETS ((p.brand, a.city), (p.brand))
ORDER BY avg_next_sqft DESC NULLS FIRST
