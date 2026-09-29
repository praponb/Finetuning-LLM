WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label,
        CASE WHEN source IN ('album', 'playlist') THEN 'group_a' WHEN source = 'radio' THEN 'group_b' ELSE 'other' END AS source_group,
        YEAR(played_at) AS played_at_year
    FROM plays
    WHERE track_id <= 9
      AND device = 'desktop'
),
play_stats AS (
    -- Per-track plays metrics
    SELECT
        track_id,
        MAX(ms_played) AS max_play_ms_played,
        SUM(ms_played) AS total_play_ms_played
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.max_play_ms_played,
        p.total_play_ms_played
    FROM scoped_plays s
    LEFT JOIN play_stats p ON p.track_id = s.track_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        played_at,
        ms_played,
        device,
        source,
        played_at_label,
        source_group,
        total_play_ms_played,
        ROUND(max_play_ms_played / NULLIF(SUM(max_play_ms_played) OVER (PARTITION BY played_at_year, track_id), 0), 4) AS max_play_ms_played_share_of_played_at_year_track,
        total_play_ms_played - LAG(total_play_ms_played) OVER w_all_by_max_play_ms_played AS total_play_ms_played_change,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY max_play_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        COUNT(*) OVER (PARTITION BY played_at_year, track_id) AS rows_in_played_at_year_track_id,
        ROW_NUMBER() OVER (ORDER BY max_play_ms_played, id) AS max_play_ms_played_seq,
        FIRST_VALUE(played_at_year) IGNORE NULLS OVER (ORDER BY max_play_ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_year
    FROM summarized_rows
    WINDOW w_all_by_max_play_ms_played AS (ORDER BY max_play_ms_played, id)
)
SELECT
    COALESCE(source, 'ALL') AS source,
    COALESCE(source_group, 'ALL') AS source_group,
    max_play_ms_played_seq,
    GROUPING_ID() AS gid,
    SUM(total_play_ms_played_change) AS total_total_play_ms_played_change,
    MAX(max_play_ms_played_seq) AS max_max_play_ms_played_seq
FROM with_trends
GROUP BY GROUPING SETS ((source, source_group, max_play_ms_played_seq), (source), (source_group))
ORDER BY source NULLS LAST, source_group NULLS LAST, max_play_ms_played_seq NULLS LAST
