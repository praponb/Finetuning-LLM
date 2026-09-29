WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        device,
        source
    FROM plays
    WHERE track_id IS NOT NULL
),
with_details_track AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.device,
        s.source,
        t.title AS track_title,
        t.is_explicit AS track_is_explicit,
        a.debut_year AS artist_debut_year
    FROM base s
    JOIN tracks t ON t.id = s.track_id
    LEFT JOIN artists a ON a.id = t.artist_id
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        listener_id,
        track_id,
        device,
        source,
        track_title,
        track_is_explicit,
        artist_debut_year,
        LAG(artist_debut_year, 3) OVER (ORDER BY artist_debut_year DESC, id) AS prev3_artist_debut_year,
        MIN(artist_debut_year) OVER (PARTITION BY device, track_is_explicit) AS device_track_is_explicit_min_artist_debut_year,
        ROUND(CUME_DIST() OVER w_track_id_by_artist_debut_yea, 3) AS artist_debut_year_cume_dist,
        ROUND(artist_debut_year / NULLIF(SUM(artist_debut_year) OVER (PARTITION BY device, track_is_explicit), 0), 4) AS artist_debut_year_share_of_device_track_is_expli,
        DENSE_RANK() OVER (PARTITION BY track_id ORDER BY artist_debut_year DESC) AS artist_debut_year_dense_rank
    FROM with_details_track
    WINDOW w_track_id_by_artist_debut_yea AS (PARTITION BY track_id ORDER BY artist_debut_year DESC)
)
-- Subtotals with ROLLUP
SELECT
    track_is_explicit,
    artist_debut_year_dense_rank,
    device,
    GROUPING_ID() AS gid,
    ROUND(STDDEV(prev3_artist_debut_year), 2) AS stddev_prev3_artist_debut_year
FROM sequenced
GROUP BY ROLLUP(track_is_explicit, artist_debut_year_dense_rank, device)
ORDER BY track_is_explicit NULLS LAST, artist_debut_year_dense_rank NULLS LAST, device NULLS LAST
