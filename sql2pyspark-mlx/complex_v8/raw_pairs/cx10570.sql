WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        DATE_TRUNC('YEAR', start_date) AS start_date_year,
        LAST_DAY(start_date) AS start_date_month_end
    FROM policies
    WHERE status IN ('CANCELLED', 'ACTIVE')
      AND YEAR(end_date) = 2026
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        start_date,
        status,
        start_date_year,
        start_date_month_end,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY start_date_month_end DESC), 3) AS start_date_month_end_percent_rank,
        DENSE_RANK() OVER (PARTITION BY status ORDER BY start_date_month_end DESC) AS start_date_month_end_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date_month_end DESC), 3) AS start_date_month_end_percent_rank_2,
        LAG(start_date) OVER (PARTITION BY holder_id, status ORDER BY start_date_year, id) AS prev_start_date,
        ROW_NUMBER() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date) ORDER BY start_date_year, id) AS start_date_year_seq,
        NTILE(10) OVER w_holder_id_start_date_month_b AS start_date_year_decile,
        FIRST_VALUE(start_date) OVER (PARTITION BY holder_id, status ORDER BY start_date_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        LAST_VALUE(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date) ORDER BY start_date_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        NTH_VALUE(start_date_year, 2) OVER (PARTITION BY status ORDER BY start_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_year,
        LAST_VALUE(start_date_month_end) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY start_date_month_end DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date_month_end,
        LAG(start_date_month_end) OVER (PARTITION BY status ORDER BY start_date_month_end DESC, id) AS prev_start_date_month_end,
        NTILE(3) OVER (PARTITION BY status ORDER BY start_date_month_end DESC, id) AS start_date_month_end_tercile
    FROM recent
    WINDOW w_holder_id_start_date_month_b AS (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date) ORDER BY start_date_year, id), w_status_by_start_date_month_e AS (PARTITION BY status ORDER BY start_date_month_end DESC)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(start_date_year_seq) OVER (PARTITION BY holder_id) AS holder_id_max_start_date_year_seq,
        LAST_VALUE(prev_start_date_month_end) OVER (PARTITION BY start_date_year_decile, TO_DATE(start_date_year) ORDER BY start_date_month_end_percent_rank_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_prev_start_date_month_end,
        ROUND(start_date_year_decile - AVG(start_date_year_decile) OVER (PARTITION BY holder_id), 2) AS start_date_year_decile_vs_holder_id_avg,
        FIRST_VALUE(start_date_month_end) IGNORE NULLS OVER (ORDER BY last_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date_month_end,
        FIRST_VALUE(start_date_month_end) OVER (ORDER BY last_start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date_month_end,
        MAX(start_date_month_end_tercile) OVER (ORDER BY start_date_month_end_percent_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_start_date_month_end_tercile_so_far
    FROM with_trends
)
-- Final projection
SELECT
    id,
    holder_id,
    start_date_year,
    start_date_month_end,
    start_date_month_end_dense_rank,
    start_date_month_end_percent_rank_2,
    start_date_year_seq,
    start_date_year_decile,
    first_start_date,
    last_start_date,
    last_known_start_date,
    second_start_date_year,
    last_start_date_month_end,
    prev_start_date_month_end,
    start_date_month_end_tercile,
    holder_id_max_start_date_year_seq,
    last_prev_start_date_month_end,
    first_known_start_date_month_end,
    first_start_date_month_end,
    max_start_date_month_end_tercile_so_far,
    COALESCE(start_date_year_decile, 0) AS start_date_year_decile_filled,
    ROUND((UNIX_TIMESTAMP(start_date_year) - UNIX_TIMESTAMP(second_start_date_year)) / 60, 1) AS minutes_second_start_date_year_to_start_date_yea
FROM with_windows
WHERE start_date_year_seq < 6
ORDER BY start_date_year DESC NULLS FIRST, start_date_month_end_tercile DESC
