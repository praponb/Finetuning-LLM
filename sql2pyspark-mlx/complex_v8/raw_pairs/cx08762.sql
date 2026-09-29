WITH recent AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        CASE WHEN ms_played >= 220498 THEN 'A' WHEN ms_played >= 176302 THEN 'B' ELSE 'C' END AS ms_played_tier
    FROM plays
    WHERE (track_id <= 2 OR is_skipped = FALSE)
),
with_parent_track AS (
    -- Enrich with dimension data
    SELECT
        s.*,
        t.is_explicit AS track_is_explicit,
        t.bpm AS track_bpm,
        t.title AS track_title,
        l.birth_year AS listener_birth_year
    FROM recent s
    LEFT JOIN tracks t ON t.id = s.track_id
    JOIN listeners l ON l.id = s.listener_id
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(track_bpm / NULLIF(SUM(track_bpm) OVER w_track_is_explicit, 0), 4) AS track_bpm_share_of_track_is_explicit,
        ROUND(days_since_played_at - AVG(days_since_played_at) OVER w_source, 2) AS days_since_played_at_vs_source_avg,
        RANK() OVER (PARTITION BY track_id, ms_played_tier ORDER BY days_since_played_at DESC) AS days_since_played_at_rank,
        LAG(listener_birth_year, 2) OVER (PARTITION BY track_id, ms_played_tier ORDER BY days_since_played_at DESC, id) AS prev2_listener_birth_year,
        COUNT(*) OVER w_source AS rows_in_source,
        ROW_NUMBER() OVER (ORDER BY days_since_played_at, id) AS days_since_played_at_seq,
        SIZE(COLLECT_SET(track_id) OVER (PARTITION BY device)) AS distinct_track_id_in_device,
        COUNT(*) OVER (PARTITION BY device) AS rows_in_device,
        NTILE(4) OVER (PARTITION BY track_id, ms_played_tier ORDER BY days_since_played_at DESC, id) AS days_since_played_at_quartile,
        ROW_NUMBER() OVER w_track_id_ms_played_tier_by_d_2 AS days_since_played_at_seq_2
    FROM with_parent_track
    WINDOW w_track_is_explicit AS (PARTITION BY track_is_explicit), w_source AS (PARTITION BY source), w_track_id_ms_played_tier_by_d_2 AS (PARTITION BY track_id, ms_played_tier ORDER BY days_since_played_at DESC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        listener_id,
        track_id,
        device,
        is_skipped,
        source,
        days_since_played_at,
        ms_played_tier,
        track_is_explicit,
        track_bpm,
        track_title,
        listener_birth_year,
        track_bpm_share_of_track_is_explicit,
        days_since_played_at_vs_source_avg,
        days_since_played_at_rank,
        prev2_listener_birth_year,
        rows_in_source,
        days_since_played_at_seq,
        distinct_track_id_in_device,
        rows_in_device,
        days_since_played_at_quartile
    FROM ranked
    WHERE days_since_played_at_seq_2 <= 2
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN rows_in_source >= 14 THEN 'large' WHEN rows_in_source >= 5 THEN 'medium' ELSE 'small' END AS rows_in_source_tier,
        source,
        is_skipped,
        SUM(CASE WHEN is_skipped = TRUE THEN 1 ELSE 0 END) AS n_is_skipped_true,
        COUNT(*) FILTER (WHERE track_is_explicit = TRUE) AS n_track_is_explicit_true,
        COUNT(*) AS row_count
    FROM leaders
    WHERE days_since_played_at_seq < 7
    GROUP BY CASE WHEN rows_in_source >= 14 THEN 'large' WHEN rows_in_source >= 5 THEN 'medium' ELSE 'small' END, source, is_skipped
    HAVING COUNT(*) >= 3
),
ranked_2 AS (
    -- Window metrics over several partitions
    SELECT
        rows_in_source_tier,
        source,
        is_skipped,
        n_track_is_explicit_true,
        RANK() OVER w_all_by_row_count_ties AS row_count_rank,
        COUNT(*) OVER (PARTITION BY row_count, n_is_skipped_true) AS rows_in_row_count_n_is_skipped_true,
        MAX(row_count) OVER (PARTITION BY n_is_skipped_true ORDER BY n_track_is_explicit_true DESC, rows_in_source_tier, source, is_skipped ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS row_count_max_3
    FROM stats
    WINDOW w_all_by_row_count_ties AS (ORDER BY row_count)
)
SELECT
    rows_in_source_tier,
    source,
    n_track_is_explicit_true,
    rows_in_row_count_n_is_skipped_true,
    row_count_max_3,
    LENGTH(TRIM(source)) AS source_len,
    CASE WHEN rows_in_source_tier IN ('large', 'small') THEN 'group_a' WHEN rows_in_source_tier = 'medium' THEN 'group_b' ELSE 'other' END AS rows_in_source_tier_group
FROM ranked_2
