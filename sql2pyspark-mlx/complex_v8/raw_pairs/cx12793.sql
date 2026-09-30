WITH valid_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(device)) AS device_len,
        LENGTH(TRIM(source)) AS source_len,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at
    FROM plays
    WHERE track_id >= 28
      AND ms_played > 92277
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(device_len) IGNORE NULLS OVER (PARTITION BY device_len, source ORDER BY days_since_played_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_device_len,
        ROUND(AVG(ms_played) OVER (PARTITION BY device_len, source ORDER BY days_since_played_at DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_3,
        ROUND(CUME_DIST() OVER w_device_len_source_by_days_si_3, 3) AS days_since_played_at_cume_dist
    FROM valid_plays
    WINDOW w_device_len_source_by_days_si_3 AS (PARTITION BY device_len, source ORDER BY days_since_played_at DESC, id)
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_len,
        s.source_len,
        s.days_since_played_at,
        s.last_known_device_len,
        s.ms_played_avg_3,
        l.birth_year AS listener_birth_year,
        l.country AS listener_country
    FROM ranked s
    JOIN listeners l ON l.id = s.listener_id
),
with_parent_2 AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_len,
        s.source_len,
        s.last_known_device_len,
        s.ms_played_avg_3,
        s.listener_country,
        t.bpm AS track_bpm
    FROM with_parent s
    JOIN tracks t ON t.id = s.track_id
    WHERE t.bpm > 94
)
SELECT id, metric, value
FROM (
    SELECT id, source_len, ms_played, last_known_device_len
    FROM with_parent_2
) src
UNPIVOT (
    value FOR metric IN (source_len AS m_source_len, ms_played AS m_ms_played, last_known_device_len AS m_last_known_device_len)
) unp
ORDER BY id, metric
