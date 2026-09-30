WITH base AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE track_id <= 23
      AND ms_played BETWEEN 95960 AND 236512
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER w_all_by_days_since_played_at, 3) AS days_since_played_at_percent_rank,
        SUM(ms_played) OVER w_device_track_id AS device_track_id_sum_ms_played,
        ROUND(PERCENT_RANK() OVER (ORDER BY days_since_played_at, id), 3) AS days_since_played_at_percent_rank_2
    FROM base
    WINDOW w_all_by_days_since_played_at AS (ORDER BY days_since_played_at), w_device_track_id AS (PARTITION BY device, track_id)
),
without_matches AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        l.signup_date AS listener_signup_date,
        l.interests AS listener_interests
    FROM without_matches s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
per_label AS (
    -- One row per listener_interest
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.source,
        s.days_since_played_at,
        s.days_since_played_at_percent_rank,
        s.device_track_id_sum_ms_played,
        s.days_since_played_at_percent_rank_2,
        s.listener_signup_date,
        t.listener_interest
    FROM combined_details s
    LATERAL VIEW OUTER EXPLODE(s.listener_interests) t AS listener_interest
)
SELECT *
FROM (
    SELECT track_id, listener_interest, days_since_played_at_percent_rank
    FROM per_label
) src
PIVOT (
    COUNT(days_since_played_at_percent_rank)
    FOR listener_interest IN ('jazz', 'live', 'kpop')
)
ORDER BY track_id
