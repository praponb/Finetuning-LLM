WITH cleaned AS (
    -- Scope the plays rows
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
    WHERE source IN ('playlist', 'search', 'album')
      AND HOUR(played_at) BETWEEN 8 AND 10
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        played_at,
        device,
        is_skipped,
        source,
        INITCAP(TRIM(device)) AS device_clean,
        CASE WHEN ms_played >= 236512 THEN 'A' WHEN ms_played >= 73177 THEN 'B' ELSE 'C' END AS ms_played_tier,
        SUBSTRING(TRIM(device), 1, 3) AS device_prefix
    FROM cleaned
),
grouped AS (
    -- Group-level totals
    SELECT
        source,
        DATE_FORMAT(played_at, 'yyyy-MM') AS played_at_ym,
        COUNT(DISTINCT ms_played_tier) AS distinct_ms_played_tiers,
        COUNT(DISTINCT source) AS distinct_sources,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE source IN ('radio', 'playlist', 'album')) AS n_source_in_radio_playlist_alb,
        MIN(played_at) AS first_played_at
    FROM derived
    WHERE played_at < TIMESTAMP '2025-03-08 10:00:00'
    GROUP BY 1, 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(distinct_ms_played_tiers) OVER (PARTITION BY TO_DATE(first_played_at), source) AS first_played_at_to_date_source_sum_distinct_ms_p,
        NTH_VALUE(distinct_ms_played_tiers, 2) OVER (PARTITION BY source, n_source_in_radio_playlist_alb ORDER BY distinct_ms_played_tiers DESC, source, played_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_ms_played_tiers,
        MAX(distinct_ms_played_tiers) OVER (PARTITION BY TO_DATE(first_played_at), source) AS first_played_at_to_date_source_max_distinct_ms_p,
        LAG(distinct_sources) OVER (PARTITION BY source, n_source_in_radio_playlist_alb ORDER BY distinct_ms_played_tiers DESC, source, played_at_ym) AS prev_distinct_sources,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids, n_source_in_radio_playlist_alb ORDER BY n_source_in_radio_playlist_alb ASC, source, played_at_ym ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS distinct_ids_sum_2,
        MIN(distinct_sources) OVER (PARTITION BY distinct_ids, n_source_in_radio_playlist_alb ORDER BY n_source_in_radio_playlist_alb ASC, source, played_at_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_distinct_sources_so_far
    FROM grouped
)
SELECT
    source,
    played_at_ym,
    distinct_ms_played_tiers,
    distinct_sources,
    distinct_ids,
    n_source_in_radio_playlist_alb,
    first_played_at,
    first_played_at_to_date_source_max_distinct_ms_p,
    distinct_ids_sum_2,
    min_distinct_sources_so_far
FROM metrics
ORDER BY first_played_at, first_played_at_to_date_source_max_distinct_ms_p DESC
