WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source
    FROM plays
    WHERE source = 'album'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped, device ORDER BY played_at NULLS LAST, id), 3) AS played_at_percent_rank,
        LAG(played_at, 3) OVER (PARTITION BY source ORDER BY played_at, id) AS prev3_played_at,
        RANK() OVER w_track_id_listener_id_by_play AS played_at_rank,
        NTILE(5) OVER (PARTITION BY source ORDER BY played_at, id) AS played_at_quintile,
        RANK() OVER (PARTITION BY is_skipped, device ORDER BY played_at NULLS LAST, id) AS played_at_rank_2,
        LEAD(played_at) OVER (PARTITION BY listener_id, device ORDER BY played_at, id) AS next_played_at,
        ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id) AS played_at_seq,
        ROW_NUMBER() OVER w_source_by_played_at AS played_at_seq_2,
        NTILE(3) OVER (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id) AS played_at_tercile
    FROM recent
    WINDOW w_track_id_listener_id_by_play AS (PARTITION BY track_id, listener_id ORDER BY played_at DESC, id), w_source_by_played_at AS (PARTITION BY source ORDER BY played_at, id)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        played_at_percent_rank,
        played_at_rank,
        played_at_quintile,
        played_at_rank_2,
        next_played_at,
        played_at_seq,
        played_at_seq_2,
        played_at_tercile,
        ROUND(AVG(played_at_rank_2) OVER (ORDER BY played_at_percent_rank ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS played_at_rank_2_avg_2_prior,
        ROUND(AVG(played_at_tercile) OVER (PARTITION BY track_id, device ORDER BY played_at_tercile DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS played_at_tercile_avg_2_prior,
        ROUND(played_at_seq_2 - AVG(played_at_seq_2) OVER (PARTITION BY played_at_rank_2, TO_DATE(prev3_played_at)), 2) AS played_at_seq_2_vs_played_at_rank_2_prev3_played,
        LAG(played_at_quintile) OVER w_played_at_month_by_played_at AS prev_played_at_quintile,
        LAG(played_at_rank) OVER (ORDER BY played_at, id) AS prev_played_at_rank
    FROM metrics
    WINDOW w_played_at_month_by_played_at AS (PARTITION BY DATE_TRUNC('MONTH', played_at) ORDER BY played_at_percent_rank DESC, id)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    device,
    source,
    played_at_percent_rank,
    played_at_rank,
    played_at_quintile,
    next_played_at,
    played_at_seq_2,
    played_at_rank_2_avg_2_prior,
    played_at_tercile_avg_2_prior,
    prev_played_at_quintile
FROM sequenced
ORDER BY prev_played_at_quintile, id
LIMIT 20
