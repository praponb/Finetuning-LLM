WITH filtered_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE SIZE(moods) >= 2
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id), 3) AS bpm_percent_rank,
        LAST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_bpm,
        LEAD(bpm, 2) OVER (PARTITION BY is_explicit ORDER BY release_date, id) AS next2_bpm,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY bpm DESC, id) AS bpm_seq,
        MAX(bpm) OVER (PARTITION BY is_explicit ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS bpm_max_3
    FROM filtered_tracks
),
latest AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        bpm_percent_rank,
        last_known_bpm,
        next2_bpm,
        bpm_seq,
        bpm_max_3
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC, id DESC) AS recency_rank
        FROM windowed
    ) ranked
    WHERE recency_rank = 1
),
with_counts AS (
    -- Attach plays metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN p.is_skipped = TRUE THEN 1 END) FROM plays p WHERE p.track_id = s.id) AS play_is_skipped_count,
        (SELECT MIN(p.played_at) FROM plays p WHERE p.track_id = s.id) AS first_played_at,
        (SELECT COUNT(*) FROM plays p WHERE p.track_id = s.id) AS play_count
    FROM latest s
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm_percent_rank,
        last_known_bpm,
        bpm_seq,
        bpm_max_3,
        play_is_skipped_count,
        first_played_at,
        play_count,
        MAX(bpm_percent_rank) OVER (PARTITION BY play_count ORDER BY bpm_percent_rank ASC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS bpm_percent_rank_max_2_prior,
        MIN(bpm) OVER (PARTITION BY MONTH(release_date)) AS release_date_month_min_bpm,
        ROUND(PERCENT_RANK() OVER (PARTITION BY play_is_skipped_count ORDER BY play_count DESC, id), 3) AS play_count_percent_rank,
        SUM(bpm_max_3) OVER w_release_date_month AS release_date_month_sum_bpm_max_3,
        MIN(last_known_bpm) OVER (PARTITION BY play_count ORDER BY bpm_percent_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_last_known_bpm_so_far,
        SIZE(COLLECT_SET(artist_id) OVER (PARTITION BY MONTH(release_date))) AS distinct_artist_id_in_release_date_month
    FROM with_counts
    WINDOW w_release_date_month AS (PARTITION BY MONTH(release_date))
),
summary AS (
    -- Summarise per group
    SELECT
        is_explicit,
        SUM(CASE WHEN LENGTH(TRIM(title)) >= 7 THEN 1 ELSE 0 END) AS n_length_trim_title_7,
        COUNT(*) FILTER (WHERE LOWER(title) LIKE '%ha%') AS n_lower_title_like_ha,
        SUM(release_date_month_min_bpm) AS total_release_date_month_min_bpm,
        ROUND(100 * COUNT(CASE WHEN UPPER(TRIM(title)) LIKE 'G%' THEN 1 END) / COUNT(*), 1) AS pct_upper_trim_title_like_g,
        COUNT(DISTINCT id) AS distinct_ids
    FROM metrics
    GROUP BY is_explicit
    HAVING AVG(release_date_month_sum_bpm_max_3) > 1128
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(n_lower_title_like_ha - AVG(n_lower_title_like_ha) OVER w_distinct_ids_n_length_trim_t, 2) AS n_lower_title_like_ha_vs_distinct_ids_n_length_t,
        COUNT(*) OVER w_distinct_ids_n_length_trim_t AS rows_in_distinct_ids_n_length_trim_title_7,
        MAX(distinct_ids) OVER (PARTITION BY n_lower_title_like_ha, n_length_trim_title_7 ORDER BY total_release_date_month_min_bpm ASC, is_explicit ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS distinct_ids_max_6_prior,
        ROUND(distinct_ids - AVG(distinct_ids) OVER (PARTITION BY distinct_ids, n_length_trim_title_7), 2) AS distinct_ids_vs_distinct_ids_n_length_trim_title
    FROM summary
    WINDOW w_distinct_ids_n_length_trim_t AS (PARTITION BY distinct_ids, n_length_trim_title_7)
)
SELECT
    is_explicit,
    n_length_trim_title_7,
    n_lower_title_like_ha,
    pct_upper_trim_title_like_g,
    distinct_ids,
    rows_in_distinct_ids_n_length_trim_title_7,
    distinct_ids_max_6_prior,
    distinct_ids_vs_distinct_ids_n_length_trim_title
FROM sequenced
ORDER BY pct_upper_trim_title_like_g DESC
