WITH cleaned_plays AS (
    -- Scope the plays rows
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
        TO_DATE(played_at) AS played_at_date
    FROM plays
    WHERE source IS NOT NULL
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        DENSE_RANK() OVER w_listener_id_by_device_len_ti AS device_len_dense_rank,
        SIZE(COLLECT_SET(source) OVER (PARTITION BY listener_id)) AS distinct_source_in_listener_id,
        FIRST_VALUE(played_at_date) IGNORE NULLS OVER (PARTITION BY listener_id ORDER BY device_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_date
    FROM cleaned_plays
    WINDOW w_listener_id_by_device_len_ti AS (PARTITION BY listener_id ORDER BY device_len ASC)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.device,
        s.is_skipped,
        s.device_len,
        s.played_at_date,
        s.device_len_dense_rank,
        s.distinct_source_in_listener_id,
        s.first_known_played_at_date,
        t.bpm AS track_bpm,
        t.title AS track_title,
        l.plan AS listener_plan
    FROM ranked s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN listeners l ON l.id = s.listener_id
    WHERE t.title IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, device_len_dense_rank, device_len, track_bpm, distinct_source_in_listener_id
    FROM combined_details
) src
UNPIVOT (
    amount FOR field IN (device_len_dense_rank AS device_len_dense_rank_v, device_len AS m_device_len, track_bpm AS m_track_bpm, distinct_source_in_listener_id AS m_distinct_source_in_listener_id)
) unp
ORDER BY id, field
