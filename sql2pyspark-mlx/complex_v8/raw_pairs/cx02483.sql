WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(source)) AS source_len
    FROM plays
    WHERE MONTH(played_at) IN (2, 3)
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(source_len / NULLIF(SUM(source_len) OVER (PARTITION BY is_skipped), 0), 4) AS source_len_share_of_is_skipped,
        source_len - LAG(source_len) OVER w_played_at_month_track_id_by AS source_len_change,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY is_skipped, device ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at,
        NTH_VALUE(played_at, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), track_id ORDER BY source_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_played_at_2,
        SUM(source_len) OVER (PARTITION BY is_skipped) AS is_skipped_sum_source_len,
        ROUND(AVG(source_len) OVER (PARTITION BY source_len ORDER BY played_at ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS source_len_avg_4,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), track_id ORDER BY source_len DESC, id) AS source_len_seq,
        MIN(source_len) OVER w_is_skipped AS is_skipped_min_source_len,
        NTILE(10) OVER w_source_len_by_played_at AS played_at_decile,
        LAST_VALUE(played_at) IGNORE NULLS OVER (PARTITION BY is_skipped ORDER BY source_len DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at
    FROM recent
    WINDOW w_played_at_month_track_id_by AS (PARTITION BY DATE_TRUNC('MONTH', played_at), track_id ORDER BY source_len DESC, id), w_is_skipped AS (PARTITION BY is_skipped), w_source_len_by_played_at AS (PARTITION BY source_len ORDER BY played_at ASC, id)
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        source_len,
        second_played_at,
        is_skipped_sum_source_len,
        source_len_avg_4,
        is_skipped_min_source_len,
        played_at_decile,
        CONCAT_WS('-', device, source) AS device_source_label,
        TO_DATE(second_played_at_2) AS second_played_at_2_date,
        CONCAT_WS('|', source, device) AS source_device_label
    FROM metrics
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        source_len,
        second_played_at,
        is_skipped_sum_source_len,
        source_len_avg_4,
        is_skipped_min_source_len,
        played_at_decile,
        second_played_at_2_date,
        source_device_label,
        ROUND(AVG(is_skipped_sum_source_len) OVER w_source_len, 2) AS source_len_avg_is_skipped_sum_source_len,
        COUNT(*) OVER w_source_len AS rows_in_source_len,
        ROUND(AVG(is_skipped_min_source_len) OVER (PARTITION BY source_len), 2) AS source_len_avg_is_skipped_min_source_len,
        ROUND(source_len / NULLIF(SUM(source_len) OVER (PARTITION BY source_len), 0), 4) AS source_len_share_of_source_len,
        NTILE(5) OVER w_device_by_second_played_at AS second_played_at_quintile
    FROM annotated
    WINDOW w_source_len AS (PARTITION BY source_len), w_device_by_second_played_at AS (PARTITION BY device ORDER BY second_played_at NULLS LAST, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', second_played_at_2_date) AS second_played_at_2_date_month,
        track_id,
        CASE WHEN rows_in_source_len >= 16 THEN 'A' WHEN rows_in_source_len >= 9 THEN 'B' ELSE 'C' END AS rows_in_source_len_tier,
        SUM(source_len) FILTER (WHERE source IN ('radio', 'playlist')) AS source_len_source_in_radio_playlist,
        ROUND(AVG(source_len_share_of_source_len), 2) AS avg_source_len_share_of_source_len,
        COUNT(*) FILTER (WHERE source IN ('search', 'playlist', 'album')) AS n_source_in_search_playlist_al,
        ARRAY_SORT(COLLECT_SET(source)) AS source_list,
        COUNT(*) AS row_count,
        COUNT(DISTINCT source) AS distinct_sources
    FROM with_windows
    GROUP BY DATE_TRUNC('MONTH', second_played_at_2_date), track_id, CASE WHEN rows_in_source_len >= 16 THEN 'A' WHEN rows_in_source_len >= 9 THEN 'B' ELSE 'C' END
),
metrics_2 AS (
    -- Window metrics over several partitions
    SELECT
        second_played_at_2_date_month,
        track_id,
        rows_in_source_len_tier,
        n_source_in_search_playlist_al,
        source_list,
        row_count,
        distinct_sources,
        ROUND(AVG(distinct_sources) OVER (ORDER BY n_source_in_search_playlist_al, second_played_at_2_date_month, track_id, rows_in_source_len_tier ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_sources_avg_1_prior,
        FIRST_VALUE(row_count) IGNORE NULLS OVER (ORDER BY n_source_in_search_playlist_al, second_played_at_2_date_month, track_id, rows_in_source_len_tier ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_row_count
    FROM per_group
)
SELECT
    second_played_at_2_date_month,
    track_id,
    rows_in_source_len_tier,
    n_source_in_search_playlist_al,
    source_list,
    row_count,
    distinct_sources_avg_1_prior,
    first_known_row_count,
    LOWER(REGEXP_REPLACE(TRIM(rows_in_source_len_tier), ' ', '_')) AS rows_in_source_len_tier_slug,
    ROUND(row_count / NULLIF(distinct_sources, 0), 3) AS row_count_per_distinct_sources
FROM metrics_2
WHERE distinct_sources_avg_1_prior < 9.1
