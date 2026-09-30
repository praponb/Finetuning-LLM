WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        start_date,
        end_date,
        status
    FROM policies
    WHERE status IN ('CANCELLED', 'EXPIRED', 'LAPSED')
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LEAD(end_date) OVER w_status_by_end_date AS next_end_date,
        LAST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date,
        ROUND(PERCENT_RANK() OVER w_status_by_end_date_ties, 3) AS end_date_percent_rank
    FROM scoped
    WINDOW w_status_by_end_date AS (PARTITION BY status ORDER BY end_date DESC, id), w_status_by_end_date_ties AS (PARTITION BY status ORDER BY end_date DESC)
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY next_end_date DESC), 3) AS next_end_date_percent_rank,
        LAST_VALUE(end_date) OVER (PARTITION BY holder_id ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_end_date
    FROM windowed
)
SELECT
    holder_id,
    status,
    GROUPING_ID() AS grouping_level,
    ROUND(SUM(next_end_date_percent_rank), 2) AS total_next_end_date_percent_rank,
    MIN(end_date_percent_rank) AS min_end_date_percent_rank
FROM ranked
GROUP BY ROLLUP(holder_id, status)
ORDER BY holder_id NULLS LAST, status NULLS LAST
