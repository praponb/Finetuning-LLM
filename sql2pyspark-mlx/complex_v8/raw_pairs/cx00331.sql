WITH valid AS (
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
        CASE WHEN ms_played >= 245815 THEN 'large' WHEN ms_played >= 99376 THEN 'medium' ELSE 'small' END AS ms_played_tier
    FROM plays
    WHERE ms_played BETWEEN 97825 AND 257789
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        is_skipped,
        source,
        ROW_NUMBER() OVER (PARTITION BY source, listener_id ORDER BY played_at DESC, id) AS rn,
        NTILE(10) OVER (PARTITION BY listener_id ORDER BY played_at, id) AS played_at_decile,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER (PARTITION BY listener_id), 0), 4) AS ms_played_share_of_listener_id,
        DENSE_RANK() OVER w_listener_id_by_played_at_tie AS played_at_dense_rank
    FROM valid
    WINDOW w_listener_id_by_played_at_tie AS (PARTITION BY listener_id ORDER BY played_at)
),
with_details_listener AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.ms_played,
        s.source,
        s.rn,
        s.played_at_decile,
        s.ms_played_share_of_listener_id,
        s.played_at_dense_rank,
        l.username AS listener_username,
        l.country AS listener_country,
        l.plan AS listener_plan
    FROM windowed s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
qualified AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM with_details_listener s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.country IS NOT NULL
              AND LOWER(l.username) LIKE '%a.%'
        )
)
SELECT
    id,
    listener_id,
    played_at,
    source,
    rn,
    ms_played_share_of_listener_id,
    played_at_dense_rank,
    listener_country,
    listener_plan
FROM qualified
