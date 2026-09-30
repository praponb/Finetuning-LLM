WITH scoped_plays AS (
    -- Filter plays
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        source,
        SUBSTRING(TRIM(source), 1, 4) AS source_prefix,
        CASE WHEN ms_played >= 261579 THEN 'HIGH' WHEN ms_played >= 73335 THEN 'MEDIUM' ELSE 'LOW' END AS ms_played_tier,
        DATE_FORMAT(played_at, 'yyyy-MM-dd HH:00') AS played_at_label
    FROM plays
    WHERE track_id >= 23
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY DATE_TRUNC('MONTH', played_at), track_id), 2) AS ms_played_vs_played_at_month_track_id_avg,
        DENSE_RANK() OVER (PARTITION BY ms_played_tier, TO_DATE(played_at) ORDER BY played_at DESC) AS played_at_dense_rank,
        ROW_NUMBER() OVER w_ms_played_tier_played_at_to_2 AS played_at_seq,
        ROW_NUMBER() OVER (ORDER BY ms_played NULLS LAST, id) AS seq_no
    FROM scoped_plays
    WINDOW w_ms_played_tier_played_at_to_2 AS (PARTITION BY ms_played_tier, TO_DATE(played_at) ORDER BY played_at DESC, id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.source,
        s.source_prefix,
        s.ms_played_tier,
        s.played_at_label,
        s.ms_played_vs_played_at_month_track_id_avg,
        s.played_at_dense_rank,
        s.played_at_seq,
        s.seq_no,
        l.country AS listener_country,
        l.plan AS listener_plan
    FROM ranked s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
with_activity AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM combined_details s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
              AND p.track_id IS NOT NULL
        )
      AND s.played_at_seq < 9
),
joined AS (
    -- Enrich with dimension data
    SELECT s.*, l.plan AS listener_plan_2, l.interests AS listener_interests
    FROM with_activity s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM joined
    WHERE seq_no = 1
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(seq_no / NULLIF(SUM(seq_no) OVER w_source_listener_country, 0), 4) AS seq_no_share_of_source_listener_country,
        FIRST_VALUE(seq_no) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_seq_no
    FROM first_rows
    WINDOW w_source_listener_country AS (PARTITION BY source, listener_country)
)
SELECT
    id,
    listener_id,
    track_id,
    source,
    source_prefix,
    ms_played_tier,
    ms_played_vs_played_at_month_track_id_avg,
    played_at_dense_rank,
    listener_country,
    listener_plan_2,
    listener_interests,
    seq_no_share_of_source_listener_country,
    first_seq_no,
    COALESCE(ms_played_tier, 'UNKNOWN') AS ms_played_tier_or_unknown,
    UPPER(TRIM(source_prefix)) AS source_prefix_clean
FROM with_windows
ORDER BY listener_id DESC, source_prefix NULLS LAST, id
LIMIT 5
