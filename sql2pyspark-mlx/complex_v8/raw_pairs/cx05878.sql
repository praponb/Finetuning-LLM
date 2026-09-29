WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE status IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        end_date,
        ROUND(PERCENT_RANK() OVER w_all_by_end_date, 3) AS end_date_percent_rank,
        LAST_VALUE(end_date) OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date,
        LEAD(end_date) OVER (ORDER BY end_date ASC, id) AS next_end_date,
        NTILE(5) OVER w_all_by_end_date AS end_date_quintile,
        ROUND(CUME_DIST() OVER (ORDER BY end_date ASC, id), 3) AS end_date_cume_dist
    FROM valid
    WINDOW w_all_by_end_date AS (ORDER BY end_date ASC, id)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        MAX(end_date_cume_dist) OVER (PARTITION BY end_date_quintile) AS end_date_quintile_max_end_date_cume_dist,
        ROUND(AVG(end_date_percent_rank) OVER (PARTITION BY end_date_quintile ORDER BY next_end_date DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS end_date_percent_rank_avg_6_prior,
        DATEDIFF(last_end_date, LAG(last_end_date) OVER w_end_date_quintile_by_next_en_2) AS days_since_prev_last_end_date,
        DENSE_RANK() OVER w_end_date_quintile_by_next_en_3 AS next_end_date_dense_rank,
        FIRST_VALUE(end_date_cume_dist) OVER (PARTITION BY end_date_quintile ORDER BY next_end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date_cume_dist
    FROM with_windows
    WINDOW w_end_date_quintile_by_next_en_2 AS (PARTITION BY end_date_quintile ORDER BY next_end_date DESC, id), w_end_date_quintile_by_next_en_3 AS (PARTITION BY end_date_quintile ORDER BY next_end_date DESC)
)
-- Final projection
SELECT
    id,
    end_date,
    end_date_percent_rank,
    next_end_date,
    end_date_quintile,
    end_date_quintile_max_end_date_cume_dist,
    days_since_prev_last_end_date,
    ABS(end_date_quintile - 5) AS end_date_quintile_distance
FROM with_windows_2
ORDER BY id DESC
