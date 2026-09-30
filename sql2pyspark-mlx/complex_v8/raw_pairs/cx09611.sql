WITH valid_warehouses AS (
    -- Filter warehouses
    SELECT
        id,
        wh_code,
        city,
        capacity,
        opened_on
    FROM warehouses
    WHERE city NOT IN ('Singapore', 'Kuala Lumpur')
),
with_counts AS (
    -- Attach stock_moves metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count,
        (SELECT COUNT(*) FROM stock_moves st WHERE st.warehouse_id = s.id) AS move_count_2
    FROM valid_warehouses s
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        move_count,
        move_count_2,
        SIZE(COLLECT_SET(city) OVER (PARTITION BY move_count, YEAR(opened_on))) AS distinct_city_in_move_count_opened_on_year,
        ROUND(CUME_DIST() OVER w_all_by_move_count, 3) AS move_count_cume_dist,
        MAX(move_count) OVER (ORDER BY move_count, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS move_count_max_3_prior,
        ROW_NUMBER() OVER (ORDER BY move_count, id) AS move_count_seq
    FROM with_counts
    WINDOW w_all_by_move_count AS (ORDER BY move_count, id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        wh_code,
        capacity,
        opened_on,
        move_count,
        move_count_2,
        distinct_city_in_move_count_opened_on_year,
        move_count_cume_dist,
        move_count_max_3_prior
    FROM with_trends
    WHERE move_count_seq = 1
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        wh_code,
        capacity,
        move_count,
        distinct_city_in_move_count_opened_on_year,
        move_count_max_3_prior,
        RANK() OVER w_move_count_2_by_move_count_2 AS move_count_2_rank,
        NTILE(3) OVER (PARTITION BY move_count_2 ORDER BY move_count_2 ASC, id) AS move_count_2_tercile,
        ROUND(AVG(move_count_2) OVER (PARTITION BY move_count_2 ORDER BY move_count_2 ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS move_count_2_avg_2_prior
    FROM leaders
    WINDOW w_move_count_2_by_move_count_2 AS (PARTITION BY move_count_2 ORDER BY move_count_2 ASC)
)
SELECT
    move_count_2_rank,
    move_count_2_tercile,
    ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(wh_code)) LIKE 'K%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_wh_code_like_k,
    COUNT(DISTINCT id) AS distinct_ids
FROM sequenced
GROUP BY ROLLUP(move_count_2_rank, move_count_2_tercile)
ORDER BY move_count_2_rank NULLS LAST, move_count_2_tercile NULLS LAST
