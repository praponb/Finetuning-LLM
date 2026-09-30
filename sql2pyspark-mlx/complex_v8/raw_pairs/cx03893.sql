WITH filtered_plays AS (
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
    WHERE TO_DATE(played_at) < DATE '2025-02-06'
      AND listener_id <= 12
),
joined AS (
    -- Join listeners
    SELECT s.*, l.plan AS listener_plan
    FROM filtered_plays s
    JOIN listeners l ON l.id = s.listener_id
),
with_details AS (
    -- Join listeners
    SELECT
        s.*,
        l.plan AS listener_plan_2,
        l.username AS listener_username,
        l.birth_year AS listener_birth_year
    FROM joined s
    JOIN listeners l ON l.id = s.listener_id AND l.plan NOT IN ('student', 'premium')
    WHERE l.birth_year < 1990
)
SELECT id, metric, value
FROM (
    SELECT id, listener_birth_year, ms_played
    FROM with_details
) src
UNPIVOT (
    value FOR metric IN (listener_birth_year AS m_listener_birth_year, ms_played AS ms_played_v)
) unp
