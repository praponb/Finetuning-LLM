WITH recent_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE track_id >= 6
      AND listener_id IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        RANK() OVER w_all_by_played_at AS played_at_rank,
        ROW_NUMBER() OVER w_played_at_month_by_played_at_2 AS played_at_seq,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY source, track_id ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY source, track_id ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source, track_id ORDER BY played_at NULLS LAST, id), 3) AS played_at_percent_rank,
        NTILE(10) OVER (PARTITION BY source, track_id ORDER BY played_at, id) AS played_at_decile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY source, track_id ORDER BY played_at), 3) AS played_at_percent_rank_2,
        ROW_NUMBER() OVER (PARTITION BY source, track_id ORDER BY played_at NULLS LAST, id) AS rn,
        ROUND(PERCENT_RANK() OVER w_all_by_played_at, 3) AS played_at_percent_rank_3,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC NULLS FIRST, id) AS played_at_quartile
    FROM recent_plays
    WINDOW w_all_by_played_at AS (ORDER BY played_at, id), w_played_at_month_by_played_at_2 AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC NULLS FIRST, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(first_known_played_at, 'yyyy-MM-dd') AS first_known_played_at_label,
        GREATEST(played_at_rank, played_at_decile) AS greatest_played_at_rank_played_at_decile,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown
    FROM ranked
    WHERE HOUR(played_at) BETWEEN 7 AND 11
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', second_played_at) AS second_played_at_month,
        COUNT(*) AS n_rows,
        MAX(second_played_at_2) AS last_second_played_at_2,
        MAX(first_known_played_at) AS last_first_known_played_at,
        ROUND(SUM(played_at_percent_rank_3) / NULLIF(SUM(played_at_quartile), 0), 3) AS played_at_percent_rank_3_to_played_at_quartile_r,
        SUM(CASE WHEN device IN ('tv', 'car') THEN 1 ELSE 0 END) AS n_device_in_tv_car
    FROM scored
    WHERE played_at_percent_rank_3 <= 0.8
    GROUP BY DATE_TRUNC('MONTH', second_played_at)
)
-- Final projection
SELECT
    second_played_at_month,
    n_rows,
    last_second_played_at_2,
    played_at_percent_rank_3_to_played_at_quartile_r,
    DATEDIFF(DATE '2026-01-01', last_second_played_at_2) AS days_since_last_second_played_at_2
FROM totals
ORDER BY second_played_at_month DESC, days_since_last_second_played_at_2
