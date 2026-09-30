WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 218129 THEN 'HIGH' WHEN ms_played >= 168782 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        COALESCE(device, 'UNKNOWN') AS device_or_unknown,
        CASE WHEN ms_played >= 261579 THEN 'A' WHEN ms_played >= 166517 THEN 'B' ELSE 'C' END AS ms_played_tier_2
    FROM plays
    WHERE device IS NOT NULL
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        s.device_or_unknown,
        l.country AS listener_country,
        l.interests AS listener_interests
    FROM recent s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
grouped AS (
    -- Group-level totals
    SELECT
        track_id,
        source,
        COUNT(*) FILTER (WHERE is_skipped = FALSE) AS n_is_skipped_false,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        ROUND(100 * COUNT(CASE WHEN device = 'speaker' THEN 1 END) / COUNT(*), 1) AS pct_device_speaker,
        COUNT(*) FILTER (WHERE device = 'phone') AS n_device_phone,
        COUNT(DISTINCT listener_country) AS distinct_listener_countrys,
        COUNT(*) FILTER (WHERE source IN ('search', 'radio')) AS n_source_in_search_radio
    FROM with_parent
    GROUP BY track_id, source
),
screened AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM grouped s
    WHERE s.track_id IN (
            SELECT p.track_id
            FROM plays p
            WHERE p.ms_played > 160264
              AND p.device NOT IN ('car', 'desktop')
        )
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.track_id) AS play_count_2,
        (SELECT COUNT(CASE WHEN p.device IN ('speaker', 'tv', 'phone') THEN 1 END) FROM plays p WHERE p.track_id = s.track_id) AS play_device_count
    FROM screened s
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY n_source_in_search_radio ASC) AS n_source_in_search_radio_dense_rank,
        ROUND(n_is_skipped_true - AVG(n_is_skipped_true) OVER (PARTITION BY play_device_count), 2) AS n_is_skipped_true_vs_play_device_count_avg,
        SUM(distinct_listener_countrys) OVER (ORDER BY play_count DESC, track_id, source ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_listener_countrys
    FROM with_counts
)
SELECT
    track_id,
    source,
    n_is_skipped_false,
    n_is_skipped_true,
    pct_device_speaker,
    n_source_in_search_radio,
    play_count,
    play_count_2,
    n_source_in_search_radio_dense_rank,
    n_is_skipped_true_vs_play_device_count_avg,
    running_distinct_listener_countrys,
    COALESCE(play_count, 0) AS play_count_filled
FROM windowed
WHERE play_count_2 > 3
ORDER BY play_count_filled DESC, source
