WITH recent_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_label,
        COALESCE(ms_played, 0) AS ms_played_filled,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
    WHERE source = 'radio'
      AND track_id IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), is_skipped) AS played_at_month_is_skipped_max_ms_played,
        ROUND(ms_played_filled - AVG(ms_played_filled) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), is_skipped), 2) AS ms_played_filled_vs_played_at_month_is_skipped_a,
        NTH_VALUE(ms_played, 2) OVER (PARTITION BY track_id ORDER BY played_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        LAST_VALUE(ms_played_filled) IGNORE NULLS OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_ms_played_filled
    FROM recent_plays
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.played_at_label,
        s.played_at_month_is_skipped_max_ms_played,
        s.ms_played_filled_vs_played_at_month_is_skipped_a,
        s.second_ms_played,
        p.id AS play_id,
        p.is_skipped AS play_is_skipped,
        p.ms_played AS play_ms_played
    FROM ranked s
    LEFT JOIN plays p ON p.track_id = s.track_id
)
SELECT id, play_id, metric, value
FROM (
    SELECT id, play_id, played_at_month_is_skipped_max_ms_played, play_ms_played
    FROM exploded_detail
) src
UNPIVOT (
    value FOR metric IN (played_at_month_is_skipped_max_ms_played, play_ms_played)
) unp
ORDER BY id, play_id, metric
