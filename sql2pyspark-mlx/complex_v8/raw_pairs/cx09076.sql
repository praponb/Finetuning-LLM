WITH recent AS (
    -- Filter stock_moves
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        CASE WHEN qty >= 372 THEN 'HIGH' WHEN qty >= 169 THEN 'MEDIUM' ELSE 'LOW' END AS qty_tier,
        FLOOR(qty / 100) * 100 AS qty_bucket
    FROM stock_moves
    WHERE move_type = 'TRANSFER'
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        GREATEST(qty, qty_bucket) AS greatest_qty_qty_bucket,
        moved_at + INTERVAL 30 MINUTES AS moved_at_plus_30m
    FROM recent
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        ROUND((UNIX_TIMESTAMP(moved_at) - UNIX_TIMESTAMP(moved_at_plus_30m)) / 60, 1) AS minutes_moved_at_plus_30m_to_moved_at,
        COALESCE(qty_tier, 'UNKNOWN') AS qty_tier_or_unknown
    FROM labeled
),
qty_tier_stats AS (
    -- Max greatest_qty_qty_bucket per qty_tier
    SELECT qty_tier, MAX(greatest_qty_qty_bucket) AS max_greatest_qty_qty_bucket
    FROM annotated
    GROUP BY qty_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_greatest_qty_qty_bucket,
        ROUND(s.greatest_qty_qty_bucket - g.max_greatest_qty_qty_bucket, 2) AS greatest_qty_qty_bucket_minus_max
    FROM annotated s
    JOIN qty_tier_stats g ON g.qty_tier = s.qty_tier
    WHERE s.greatest_qty_qty_bucket = g.max_greatest_qty_qty_bucket
)
-- Final projection
SELECT
    id,
    moved_at,
    greatest_qty_qty_bucket,
    moved_at_plus_30m,
    minutes_moved_at_plus_30m_to_moved_at,
    max_greatest_qty_qty_bucket,
    SUBSTRING(TRIM(move_type), 1, 2) AS move_type_prefix
FROM compared
ORDER BY moved_at
