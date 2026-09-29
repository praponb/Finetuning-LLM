WITH recent_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        MONTH(played_at) AS played_at_month,
        CASE WHEN ms_played >= 318978 THEN 'large' WHEN ms_played >= 145720 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE listener_id IS NOT NULL
      AND device = 'phone'
),
joined_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.played_at_month,
        s.ms_played_tier,
        l.interests AS listener_interests,
        l.username AS listener_username,
        t.is_explicit AS track_is_explicit
    FROM recent_plays s
    JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
exploded AS (
    -- One row per listener_interest
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.played_at_month,
        s.ms_played_tier,
        s.listener_username,
        s.track_is_explicit,
        t.listener_interest_pos,
        t.listener_interest
    FROM joined_listener s
    LATERAL VIEW POSEXPLODE(s.listener_interests) t AS listener_interest_pos, listener_interest
)
-- Subtotals with ROLLUP
SELECT
    listener_id,
    track_is_explicit,
    MIN(played_at_month) AS min_played_at_month,
    ROUND(AVG(listener_interest_pos), 2) AS avg_listener_interest_pos,
    MIN(ms_played) AS min_ms_played
FROM exploded
GROUP BY ROLLUP(listener_id, track_is_explicit)
