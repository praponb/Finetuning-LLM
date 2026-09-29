WITH valid AS (
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
        CONCAT_WS(': ', device, source) AS device_source_label
    FROM plays
    WHERE (listener_id IS NOT NULL OR source <> 'search')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.device,
        s.is_skipped,
        s.source,
        s.device_source_label,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title,
        a.debut_year AS artist_debut_year,
        a.country AS artist_country
    FROM valid s
    JOIN tracks t ON t.id = s.track_id
    JOIN artists a ON a.id = t.artist_id
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(artist_debut_year) OVER (PARTITION BY artist_country) AS artist_country_max_artist_debut_year,
        NTH_VALUE(artist_debut_year, 2) OVER (ORDER BY played_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year,
        ROUND(ms_played - AVG(ms_played) OVER (PARTITION BY artist_country), 2) AS ms_played_vs_artist_country_avg,
        NTILE(10) OVER (PARTITION BY is_skipped ORDER BY artist_debut_year, id) AS artist_debut_year_decile,
        MAX(ms_played) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS ms_played_max_2,
        RANK() OVER w_all_by_artist_debut_year AS artist_debut_year_rank,
        NTH_VALUE(artist_debut_year, 2) OVER (PARTITION BY MONTH(played_at), track_is_explicit ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_artist_debut_year_2,
        MAX(ms_played) OVER (PARTITION BY artist_country) AS artist_country_max_ms_played,
        LAST_VALUE(artist_debut_year) OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_artist_debut_year,
        LAG(played_at) OVER (PARTITION BY is_skipped ORDER BY artist_debut_year, id) AS prev_played_at,
        ROUND(AVG(ms_played) OVER (PARTITION BY artist_country), 2) AS artist_country_avg_ms_played,
        SIZE(COLLECT_SET(source) OVER w_artist_country) AS distinct_source_in_artist_country,
        FIRST_VALUE(artist_debut_year) OVER (PARTITION BY is_skipped ORDER BY artist_debut_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_artist_debut_year
    FROM with_parent
    WINDOW w_artist_country AS (PARTITION BY artist_country), w_all_by_artist_debut_year AS (ORDER BY artist_debut_year DESC, id)
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        track_id,
        is_skipped,
        source,
        device_source_label,
        track_is_explicit,
        track_title,
        artist_debut_year,
        artist_country,
        artist_country_max_artist_debut_year,
        artist_debut_year_decile,
        ms_played_max_2,
        artist_country_max_ms_played,
        artist_country_avg_ms_played,
        distinct_source_in_artist_country,
        first_artist_debut_year,
        MIN(ms_played_max_2) OVER w_artist_debut_year_decile_tra AS artist_debut_year_decile_track_is_explicit_min_m,
        ROUND(second_artist_debut_year_2 - AVG(second_artist_debut_year_2) OVER (PARTITION BY artist_debut_year_decile, track_is_explicit), 2) AS second_artist_debut_year_2_vs_artist_debut_year_,
        MAX(artist_country_max_ms_played) OVER (PARTITION BY artist_debut_year_decile, track_is_explicit) AS artist_debut_year_decile_track_is_explicit_max_a,
        MAX(artist_debut_year_rank) OVER (PARTITION BY is_skipped, source ORDER BY artist_debut_year_rank, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS artist_debut_year_rank_max_3_prior,
        LEAD(artist_debut_year_decile) OVER (ORDER BY ms_played_vs_artist_country_avg, id) AS next_artist_debut_year_decile,
        LEAD(artist_country_avg_ms_played, 3) OVER w_is_skipped_source_by_artist_2 AS next3_artist_country_avg_ms_played,
        COUNT(*) OVER (PARTITION BY artist_debut_year_decile, track_is_explicit) AS rows_in_artist_debut_year_decile_track_is_explic,
        NTH_VALUE(ms_played_max_2, 2) OVER (PARTITION BY is_skipped, source ORDER BY artist_debut_year_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_ms_played_max_2
    FROM sequenced
    WINDOW w_artist_debut_year_decile_tra AS (PARTITION BY artist_debut_year_decile, track_is_explicit), w_is_skipped_source_by_artist_2 AS (PARTITION BY is_skipped, source ORDER BY artist_debut_year_rank, id)
)
SELECT
    id,
    listener_id,
    track_id,
    is_skipped,
    source,
    device_source_label,
    artist_country,
    artist_debut_year_decile,
    ms_played_max_2,
    artist_country_max_ms_played,
    artist_country_avg_ms_played,
    distinct_source_in_artist_country,
    first_artist_debut_year,
    artist_debut_year_decile_track_is_explicit_max_a,
    next_artist_debut_year_decile,
    next3_artist_country_avg_ms_played,
    second_ms_played_max_2
FROM windowed
