WITH recent AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(premium, 0.0) AS premium_filled,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM policies
    WHERE premium < 2945.9
      AND status = 'LAPSED'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        start_date,
        premium_filled,
        status_or_unknown,
        NTH_VALUE(premium_filled, 2) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_filled,
        MIN(premium) OVER (ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(AVG(premium_filled) OVER (PARTITION BY status_or_unknown, holder_id), 2) AS status_or_unknown_holder_id_avg_premium_filled,
        ROUND(premium_filled - AVG(premium_filled) OVER (PARTITION BY status_or_unknown, holder_id), 2) AS premium_filled_vs_status_or_unknown_holder_id_av
    FROM recent
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        start_date,
        status_or_unknown,
        second_premium_filled,
        status_or_unknown_holder_id_avg_premium_filled,
        premium_filled_vs_status_or_unknown_holder_id_av,
        RANK() OVER (PARTITION BY status_or_unknown ORDER BY second_premium_filled DESC) AS second_premium_filled_rank,
        premium_filled - LAG(premium_filled) OVER w_status_or_unknown_by_second_2 AS premium_filled_change
    FROM windowed
    WINDOW w_status_or_unknown_by_second_2 AS (PARTITION BY status_or_unknown ORDER BY second_premium_filled, id)
),
top_per_group AS (
    -- First row per group
    SELECT
        id,
        start_date,
        status_or_unknown,
        second_premium_filled,
        status_or_unknown_holder_id_avg_premium_filled,
        premium_filled_vs_status_or_unknown_holder_id_av,
        premium_filled_change
    FROM metrics
    WHERE second_premium_filled_rank = 1
)
-- Final projection
SELECT
    id,
    start_date,
    status_or_unknown,
    second_premium_filled,
    status_or_unknown_holder_id_avg_premium_filled,
    premium_filled_vs_status_or_unknown_holder_id_av,
    QUARTER(start_date) AS start_date_quarter,
    SUBSTRING(TRIM(status_or_unknown), 1, 3) AS status_or_unknown_prefix
FROM top_per_group
WHERE status_or_unknown IN ('UNKNOWN', 'CANCELLED', 'LAPSED')
ORDER BY status_or_unknown, start_date_quarter
