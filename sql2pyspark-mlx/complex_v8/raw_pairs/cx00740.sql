WITH scoped_plays AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        is_skipped,
        source,
        DATE_TRUNC('QUARTER', played_at) AS played_at_quarter,
        LOWER(TRIM(device)) AS device_clean,
        CONCAT_WS(' / ', device, source) AS device_source_label
    FROM plays
    WHERE source IN ('album', 'radio', 'search')
      AND played_at < TIMESTAMP '2025-03-02 10:00:00'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        source,
        played_at_quarter,
        device_clean,
        device_source_label,
        CASE WHEN is_skipped THEN 1 ELSE 0 END AS is_skipped_flag,
        UPPER(TRIM(device_source_label)) AS device_source_label_clean,
        CONCAT(source, '#', CAST(track_id AS STRING)) AS source_ref
    FROM scoped_plays
),
filtered AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND HOUR(p.played_at) BETWEEN 9 AND 14
        )
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        listener_id,
        played_at,
        is_skipped,
        played_at_quarter,
        device_clean,
        device_source_label,
        is_skipped_flag,
        device_source_label_clean,
        source_ref,
        ROUND(CUME_DIST() OVER (PARTITION BY listener_id, is_skipped_flag ORDER BY played_at_quarter DESC), 3) AS played_at_quarter_cume_dist,
        ROW_NUMBER() OVER w_source_is_skipped_flag_by_pl AS played_at_quarter_seq,
        ROUND(is_skipped_flag / NULLIF(SUM(is_skipped_flag) OVER w_is_skipped_played_at_year, 0), 4) AS is_skipped_flag_share_of_is_skipped_played_at_ye,
        NTH_VALUE(is_skipped_flag, 2) OVER (PARTITION BY source, is_skipped ORDER BY is_skipped_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_is_skipped_flag,
        LAST_VALUE(played_at_quarter) IGNORE NULLS OVER (PARTITION BY listener_id, is_skipped_flag ORDER BY played_at_quarter DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_quarter,
        COUNT(*) OVER w_is_skipped_played_at_year AS rows_in_is_skipped_played_at_year
    FROM filtered
    WINDOW w_source_is_skipped_flag_by_pl AS (PARTITION BY source, is_skipped_flag ORDER BY played_at_quarter, id), w_is_skipped_played_at_year AS (PARTITION BY is_skipped, YEAR(played_at))
),
joined_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.device_source_label,
        s.is_skipped_flag,
        s.device_source_label_clean,
        s.source_ref,
        s.played_at_quarter_cume_dist,
        s.played_at_quarter_seq,
        s.is_skipped_flag_share_of_is_skipped_played_at_ye,
        s.last_known_played_at_quarter,
        s.rows_in_is_skipped_played_at_year,
        l.signup_date AS listener_signup_date
    FROM windowed s
    JOIN listeners l ON l.id = s.listener_id
),
rollup_base AS (
    -- Summarise per group
    SELECT
        YEAR(last_known_played_at_quarter) AS last_known_played_at_quarter_year,
        played_at_quarter_seq,
        DATE_TRUNC('MONTH', played_at) AS played_at_month,
        COUNT(*) AS n_rows,
        ROUND(SUM(played_at_quarter_seq) / NULLIF(SUM(rows_in_is_skipped_played_at_year), 0), 3) AS played_at_quarter_seq_to_rows_in_is_skipped_play,
        MIN(played_at_quarter_cume_dist) AS min_played_at_quarter_cume_dist,
        COUNT(DISTINCT listener_id) AS distinct_listener_ids,
        COUNT(DISTINCT id) AS distinct_ids
    FROM joined_listener
    WHERE YEAR(listener_signup_date) = 2024
    GROUP BY YEAR(last_known_played_at_quarter), played_at_quarter_seq, DATE_TRUNC('MONTH', played_at)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        last_known_played_at_quarter_year,
        played_at_quarter_seq,
        played_at_month,
        played_at_quarter_seq_to_rows_in_is_skipped_play,
        min_played_at_quarter_cume_dist,
        distinct_listener_ids,
        SUM(distinct_listener_ids) OVER w_n_rows AS n_rows_sum_distinct_listener_ids,
        MIN(played_at_quarter_seq) OVER (PARTITION BY n_rows) AS n_rows_min_played_at_quarter_seq
    FROM rollup_base
    WINDOW w_n_rows AS (PARTITION BY n_rows)
)
-- Final projection
SELECT
    last_known_played_at_quarter_year,
    played_at_quarter_seq,
    played_at_quarter_seq_to_rows_in_is_skipped_play,
    min_played_at_quarter_cume_dist,
    distinct_listener_ids,
    n_rows_min_played_at_quarter_seq
FROM with_trends
WHERE distinct_listener_ids >= 4
ORDER BY last_known_played_at_quarter_year, min_played_at_quarter_cume_dist DESC
