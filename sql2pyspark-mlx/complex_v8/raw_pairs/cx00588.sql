WITH valid AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        source,
        HOUR(played_at) AS played_at_hour,
        DATEDIFF(DATE '2025-06-30', played_at) AS days_since_played_at,
        CASE WHEN source IN ('album', 'playlist') THEN 'group_a' WHEN source = 'search' THEN 'group_b' ELSE 'other' END AS source_group
    FROM plays
    WHERE track_id >= 6
      AND is_skipped = TRUE
),
windowed AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(played_at_hour - AVG(played_at_hour) OVER w_track_id_source, 2) AS played_at_hour_vs_track_id_source_avg,
        COUNT(*) OVER w_track_id_source AS rows_in_track_id_source,
        ROW_NUMBER() OVER (PARTITION BY track_id, listener_id ORDER BY days_since_played_at DESC, id) AS seq_no
    FROM valid
    WINDOW w_track_id_source AS (PARTITION BY track_id, source)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(seq_no, 0) AS seq_no_filled,
        LOWER(REGEXP_REPLACE(TRIM(source_group), ' ', '_')) AS source_group_slug
    FROM windowed
)
SELECT
    source,
    played_at_hour,
    seq_no,
    GROUPING_ID() AS gid,
    COUNT(*) AS n_rows,
    MAX(played_at) AS last_played_at,
    COUNT(DISTINCT source_group) AS distinct_source_groups
FROM scored
GROUP BY ROLLUP(source, played_at_hour, seq_no)
ORDER BY source NULLS LAST, played_at_hour NULLS LAST, seq_no NULLS LAST
