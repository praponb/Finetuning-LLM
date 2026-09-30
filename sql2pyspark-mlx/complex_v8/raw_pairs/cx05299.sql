WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
    WHERE MONTH(played_at) IN (2, 3)
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY track_id ORDER BY ms_played), 3) AS ms_played_percent_rank,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_is_skipped_track_id, 0), 4) AS ms_played_share_of_is_skipped_track_id,
        ROUND(CUME_DIST() OVER w_track_id_by_ms_played_ties, 3) AS ms_played_cume_dist
    FROM valid_plays
    WINDOW w_is_skipped_track_id AS (PARTITION BY is_skipped, track_id), w_track_id_by_ms_played_ties AS (PARTITION BY track_id ORDER BY ms_played)
),
joined_listener AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.plan AS listener_plan,
        l.username AS listener_username,
        l.interests AS listener_interests
    FROM ranked s
    LEFT JOIN listeners l ON l.id = s.listener_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, ms_played_percent_rank, ms_played_cume_dist
    FROM joined_listener
) src
UNPIVOT (
    amount FOR measure IN (ms_played_percent_rank, ms_played_cume_dist)
) unp
