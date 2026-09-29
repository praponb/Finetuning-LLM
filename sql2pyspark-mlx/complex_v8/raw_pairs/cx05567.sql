WITH scoped_moves AS (
    -- Scope the stock_moves rows
    SELECT
        id,
        warehouse_id,
        sku_id,
        move_type,
        qty,
        moved_at,
        unit_price,
        HOUR(moved_at) AS moved_at_hour
    FROM stock_moves
    WHERE warehouse_id IS NOT NULL
),
summary AS (
    -- Group-level totals
    SELECT
        moved_at_hour,
        ARRAY_SORT(COLLECT_SET(move_type)) AS move_type_list,
        COUNT(*) AS cnt,
        MAX(moved_at) AS last_moved_at,
        COUNT(DISTINCT id) AS distinct_ids,
        ROUND(100 * COUNT(CASE WHEN move_type IS NOT NULL THEN 1 END) / COUNT(*), 1) AS pct_move_type_is_not_null
    FROM scoped_moves
    WHERE sku_id <= 6
    GROUP BY 1
),
with_activity AS (
    -- Compare against the overall avg
    SELECT *
    FROM summary
    WHERE moved_at_hour > (SELECT AVG(moved_at_hour) FROM summary)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(moved_at_hour, 2) OVER (ORDER BY pct_move_type_is_not_null ASC, moved_at_hour) AS next2_moved_at_hour,
        MIN(distinct_ids) OVER (PARTITION BY moved_at_hour, cnt) AS moved_at_hour_cnt_min_distinct_ids,
        ROW_NUMBER() OVER (PARTITION BY moved_at_hour, cnt ORDER BY cnt, moved_at_hour) AS cnt_seq
    FROM with_activity
),
tagged AS (
    -- Add calculated columns
    SELECT
        moved_at_hour,
        move_type_list,
        cnt,
        last_moved_at,
        distinct_ids,
        pct_move_type_is_not_null,
        next2_moved_at_hour,
        moved_at_hour_cnt_min_distinct_ids,
        cnt_seq,
        MONTH(last_moved_at) AS last_moved_at_month
    FROM windowed
)
-- Native SQL UNPIVOT (wide to long)
SELECT moved_at_hour, field, reading
FROM (
    SELECT moved_at_hour, last_moved_at_month, distinct_ids, cnt, cnt_seq
    FROM tagged
) src
UNPIVOT (
    reading FOR field IN (last_moved_at_month, distinct_ids, cnt, cnt_seq)
) unp
