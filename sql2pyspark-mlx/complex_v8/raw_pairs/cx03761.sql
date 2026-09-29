WITH filtered AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source
    FROM plays
),
joined AS (
    -- Join listeners
    SELECT
        s.*,
        l.interests AS listener_interests,
        l.plan AS listener_plan,
        t.title AS track_title,
        t.bpm AS track_bpm
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        track_id,
        played_at,
        is_skipped,
        listener_interests,
        track_title,
        track_bpm,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(played_at), listener_plan ORDER BY track_bpm) AS track_bpm_dense_rank,
        ROUND(AVG(track_bpm) OVER (PARTITION BY TO_DATE(played_at)), 2) AS played_at_to_date_avg_track_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_skipped, listener_id ORDER BY played_at ASC), 3) AS played_at_percent_rank,
        COUNT(*) OVER w_played_at_to_date AS rows_in_played_at_to_date,
        FIRST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY is_skipped, listener_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at,
        SUM(track_bpm) OVER (PARTITION BY TO_DATE(played_at)) AS played_at_to_date_sum_track_bpm,
        LEAD(track_bpm) OVER (PARTITION BY track_id ORDER BY track_bpm DESC, id) AS next_track_bpm,
        LAST_VALUE(track_bpm) IGNORE NULLS OVER (PARTITION BY track_id, is_skipped ORDER BY played_at NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_track_bpm,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped, listener_id ORDER BY played_at ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at
    FROM joined
    WINDOW w_played_at_to_date AS (PARTITION BY TO_DATE(played_at))
)
SELECT
    id,
    track_id,
    is_skipped,
    listener_interests,
    track_title,
    track_bpm,
    track_bpm_dense_rank,
    played_at_to_date_avg_track_bpm,
    rows_in_played_at_to_date,
    first_known_played_at,
    played_at_to_date_sum_track_bpm,
    next_track_bpm,
    last_known_track_bpm,
    second_played_at
FROM with_trends
WHERE MONTH(played_at) IN (1, 3)
