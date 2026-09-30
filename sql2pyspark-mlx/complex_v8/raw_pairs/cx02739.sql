WITH scoped_plays AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        is_skipped,
        source,
        CASE WHEN device IN ('tv', 'phone') THEN 'group_a' WHEN device = 'speaker' THEN 'group_b' ELSE 'other' END AS device_group,
        DATE_TRUNC('YEAR', played_at) AS played_at_year
    FROM plays
    WHERE source NOT IN ('search', 'radio')
      AND played_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_parent_track AS (
    -- Join tracks
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.is_skipped,
        s.source,
        s.device_group,
        s.played_at_year,
        t.is_explicit AS track_is_explicit
    FROM scoped_plays s
    LEFT JOIN tracks t ON t.id = s.track_id AND NOT ARRAY_CONTAINS(t.moods, 'chill')
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        source,
        device_group,
        played_at_year,
        track_is_explicit,
        ROUND(PERCENT_RANK() OVER w_is_skipped_track_is_explicit, 3) AS played_at_year_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY is_skipped, track_is_explicit ORDER BY played_at_year), 3) AS played_at_year_cume_dist,
        DENSE_RANK() OVER (PARTITION BY is_skipped, track_is_explicit ORDER BY played_at_year, id) AS played_at_year_dense_rank,
        LAST_VALUE(played_at_year) OVER (PARTITION BY track_id ORDER BY played_at_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_played_at_year,
        RANK() OVER w_track_id_by_played_at_year AS played_at_year_rank,
        LEAD(played_at_year, 3) OVER (PARTITION BY track_id ORDER BY played_at_year, id) AS next3_played_at_year,
        LAST_VALUE(played_at_year) IGNORE NULLS OVER (PARTITION BY is_skipped, track_is_explicit ORDER BY played_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_played_at_year,
        LEAD(played_at_year) OVER (PARTITION BY track_id ORDER BY played_at_year, id) AS next_played_at_year
    FROM with_parent_track
    WINDOW w_is_skipped_track_is_explicit AS (PARTITION BY is_skipped, track_is_explicit ORDER BY played_at_year), w_track_id_by_played_at_year AS (PARTITION BY track_id ORDER BY played_at_year, id)
),
ranked AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        source,
        device_group,
        played_at_year,
        played_at_year_cume_dist,
        played_at_year_rank,
        next_played_at_year,
        NTILE(4) OVER (PARTITION BY track_is_explicit ORDER BY last_known_played_at_year, id) AS last_known_played_at_year_quartile,
        ROUND(played_at_year_rank - AVG(played_at_year_rank) OVER w_track_is_explicit, 2) AS played_at_year_rank_vs_track_is_explicit_avg,
        FIRST_VALUE(played_at_year_rank) IGNORE NULLS OVER (PARTITION BY listener_id, TO_DATE(played_at_year) ORDER BY last_played_at_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_played_at_year_rank,
        LAG(played_at_year_percent_rank) OVER w_listener_id_played_at_year_t_2 AS prev_played_at_year_percent_rank,
        ROUND(played_at_year_percent_rank - AVG(played_at_year_percent_rank) OVER (PARTITION BY track_is_explicit), 2) AS played_at_year_percent_rank_vs_track_is_explicit,
        played_at_year_rank - LAG(played_at_year_rank) OVER (PARTITION BY listener_id, TO_DATE(played_at_year) ORDER BY last_played_at_year, id) AS played_at_year_rank_change,
        MIN(played_at_year_percent_rank) OVER (PARTITION BY track_is_explicit) AS track_is_explicit_min_played_at_year_percent_ran,
        LAG(last_played_at_year) OVER (PARTITION BY listener_id, TO_DATE(played_at_year) ORDER BY last_played_at_year, id) AS prev_last_played_at_year
    FROM with_windows
    WINDOW w_track_is_explicit AS (PARTITION BY track_is_explicit), w_listener_id_played_at_year_t_2 AS (PARTITION BY listener_id, TO_DATE(played_at_year) ORDER BY last_played_at_year, id)
)
SELECT
    id,
    listener_id,
    source,
    device_group,
    played_at_year_cume_dist,
    played_at_year_rank,
    next_played_at_year,
    last_known_played_at_year_quartile,
    played_at_year_rank_vs_track_is_explicit_avg,
    first_known_played_at_year_rank,
    prev_played_at_year_percent_rank,
    played_at_year_percent_rank_vs_track_is_explicit,
    played_at_year_rank_change,
    track_is_explicit_min_played_at_year_percent_ran,
    prev_last_played_at_year
FROM ranked
WHERE prev_played_at_year_percent_rank <= 0.8
ORDER BY listener_id, id
