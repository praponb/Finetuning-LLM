WITH valid AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        device,
        is_skipped
    FROM plays
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        t.moods AS track_moods,
        a.artist_name,
        a.genre AS artist_genre
    FROM valid s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY artist_genre, is_skipped) AS rows_in_artist_genre_is_skipped,
        RANK() OVER (PARTITION BY artist_genre, is_skipped ORDER BY ms_played DESC NULLS FIRST, id) AS ms_played_rank,
        SIZE(COLLECT_SET(listener_id) OVER w_is_skipped_track_id) AS distinct_listener_id_in_is_skipped_track_id,
        LAST_VALUE(ms_played) OVER (PARTITION BY artist_genre, is_skipped ORDER BY ms_played DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_ms_played,
        NTH_VALUE(ms_played, 2) OVER (ORDER BY ms_played, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played,
        ROUND(CUME_DIST() OVER w_track_id_is_skipped_by_ms_pl, 3) AS ms_played_cume_dist,
        LAG(ms_played, 3) OVER (PARTITION BY track_id, listener_id ORDER BY ms_played ASC, id) AS prev3_ms_played,
        ms_played - LAG(ms_played) OVER (PARTITION BY track_id, is_skipped ORDER BY ms_played, id) AS ms_played_change,
        SIZE(COLLECT_SET(artist_genre) OVER w_is_skipped_track_id) AS distinct_artist_genre_in_is_skipped_track_id,
        ROUND(AVG(ms_played) OVER (PARTITION BY track_id, is_skipped ORDER BY ms_played, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_2_prior,
        MAX(ms_played) OVER (PARTITION BY artist_genre, is_skipped) AS artist_genre_is_skipped_max_ms_played
    FROM enriched
    WINDOW w_is_skipped_track_id AS (PARTITION BY is_skipped, track_id), w_track_id_is_skipped_by_ms_pl AS (PARTITION BY track_id, is_skipped ORDER BY ms_played, id)
),
top_rows AS (
    -- First row per group
    SELECT *
    FROM ranked
    WHERE ms_played_rank = 1
      AND distinct_listener_id_in_is_skipped_track_id < 8
),
rollup_base AS (
    -- Summarise per group
    SELECT
        listener_id,
        MIN(ms_played_avg_2_prior) AS min_ms_played_avg_2_prior,
        ROUND(SUM(second_ms_played) / NULLIF(SUM(ms_played), 0), 3) AS second_ms_played_to_ms_played_ratio,
        PERCENTILE_APPROX(ms_played, 0.5) AS median_ms_played
    FROM top_rows
    GROUP BY listener_id
    HAVING SUM(distinct_artist_genre_in_is_skipped_track_id) > 3
)
SELECT
    listener_id,
    median_ms_played,
    COALESCE(median_ms_played, 0) AS median_ms_played_filled
FROM rollup_base
ORDER BY listener_id DESC
