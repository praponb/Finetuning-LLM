WITH valid_plays AS (
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
        played_at + INTERVAL 15 MINUTES AS played_at_plus_15m,
        FLOOR(ms_played / 50000) * 50000 AS ms_played_bucket
    FROM plays
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        played_at_plus_15m,
        MIN(ms_played_bucket) OVER (PARTITION BY MONTH(played_at), track_id) AS played_at_month_track_id_min_ms_played_bucket,
        ROUND(ms_played_bucket / NULLIF(SUM(ms_played_bucket) OVER (PARTITION BY MONTH(played_at), track_id), 0), 4) AS ms_played_bucket_share_of_played_at_month_track_,
        ROUND(AVG(ms_played_bucket) OVER (PARTITION BY device ORDER BY ms_played_bucket ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS ms_played_bucket_avg_1_prior,
        RANK() OVER (ORDER BY ms_played DESC, id) AS ms_played_rank,
        ROW_NUMBER() OVER (PARTITION BY listener_id, track_id ORDER BY ms_played DESC, id) AS seq_no,
        LAG(played_at) OVER w_all_by_ms_played AS prev_played_at
    FROM valid_plays
    WINDOW w_all_by_ms_played AS (ORDER BY ms_played DESC, id)
),
combined_details AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        s.played_at_plus_15m,
        s.seq_no,
        s.prev_played_at,
        l.birth_year AS listener_birth_year,
        l.country AS listener_country,
        l.plan AS listener_plan,
        t.is_explicit AS track_is_explicit
    FROM sequenced s
    LEFT JOIN listeners l ON l.id = s.listener_id
    LEFT JOIN tracks t ON t.id = s.track_id
),
qualified AS (
    -- Rows that have matching plays
    SELECT s.*
    FROM combined_details s
    WHERE EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.listener_id = s.listener_id
        )
)
SELECT
    listener_plan,
    listener_country,
    is_skipped,
    GROUPING_ID() AS grouping_level,
    MIN(played_at_plus_15m) AS first_played_at_plus_15m,
    SUM(CASE WHEN listener_country NOT IN ('UK', 'JP') THEN seq_no ELSE 0 END) AS seq_no_listener_country_not_in_uk_j
FROM qualified
GROUP BY CUBE(listener_plan, listener_country, is_skipped)
