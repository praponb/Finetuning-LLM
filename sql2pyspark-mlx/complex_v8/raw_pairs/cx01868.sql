WITH scoped AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'radio'
      AND ms_played >= 71476
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (ORDER BY ms_played DESC, id) AS ms_played_decile,
        MIN(ms_played) OVER (PARTITION BY listener_id, is_skipped) AS listener_id_is_skipped_min_ms_played,
        SUM(ms_played) OVER (PARTITION BY track_id ORDER BY played_at ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS ms_played_sum_3,
        ROUND(AVG(ms_played) OVER (ORDER BY played_at DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        MAX(ms_played) OVER (ORDER BY played_at DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS ms_played_max_2_prior
    FROM scoped
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        ms_played_decile,
        ms_played_sum_3,
        ms_played_avg_7,
        ms_played_max_2_prior,
        FIRST_VALUE(ms_played_decile) IGNORE NULLS OVER (PARTITION BY ms_played_decile ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ms_played_decile,
        LAG(ms_played_max_2_prior) OVER w_played_at_month_by_played_at AS prev_ms_played_max_2_prior,
        COUNT(*) OVER (PARTITION BY ms_played_decile) AS rows_in_ms_played_decile,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at
    FROM windowed
    WINDOW w_played_at_month_by_played_at AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at DESC, id)
)
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    ms_played_decile,
    ms_played_sum_3,
    ms_played_avg_7,
    ms_played_max_2_prior,
    prev_ms_played_max_2_prior,
    rows_in_ms_played_decile,
    last_known_played_at
FROM with_windows
ORDER BY id
