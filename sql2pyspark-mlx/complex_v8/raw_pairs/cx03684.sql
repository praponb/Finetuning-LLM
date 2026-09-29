WITH valid_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source
    FROM plays
    WHERE HOUR(played_at) BETWEEN 9 AND 13
),
screened AS (
    -- Keep rows whose listener matches
    SELECT s.*
    FROM valid_plays s
    WHERE EXISTS (
            SELECT 1
            FROM listeners l
            WHERE l.id = s.listener_id
              AND l.birth_year < 1986
              AND ARRAY_CONTAINS(l.interests, 'kpop')
        )
)
SELECT
    listener_id,
    track_id,
    device,
    ROUND(100 * COUNT(CASE WHEN source = 'search' THEN 1 END) / COUNT(*), 1) AS pct_source_search,
    COUNT(*) FILTER (WHERE device = 'phone') AS n_device_phone,
    ROUND(AVG(ms_played), 2) AS avg_ms_played
FROM screened
GROUP BY GROUPING SETS ((listener_id, track_id, device), (listener_id), ())
ORDER BY listener_id NULLS LAST, track_id NULLS LAST, device NULLS LAST
