WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm
    FROM tracks
),
with_details_artist AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.bpm,
        a.artist_name,
        a.country AS artist_country
    FROM valid s
    JOIN artists a ON a.id = s.artist_id AND a.genre = 'Pop'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(bpm) OVER w_artist_id, 2) AS artist_id_avg_bpm,
        ROUND(PERCENT_RANK() OVER (ORDER BY bpm, id), 3) AS bpm_percent_rank,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id, artist_country ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        DENSE_RANK() OVER (ORDER BY bpm, id) AS bpm_dense_rank,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        ROUND(CUME_DIST() OVER w_artist_country_artist_id_by, 3) AS bpm_cume_dist,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_country, artist_id ORDER BY bpm ASC), 3) AS bpm_percent_rank_2,
        DENSE_RANK() OVER (PARTITION BY artist_country ORDER BY bpm DESC) AS bpm_dense_rank_2,
        NTILE(10) OVER (ORDER BY bpm, id) AS bpm_decile,
        NTILE(5) OVER (PARTITION BY artist_country ORDER BY bpm DESC, id) AS bpm_quintile,
        NTILE(4) OVER (PARTITION BY artist_id, artist_country ORDER BY bpm DESC, id) AS bpm_quartile,
        bpm - LAG(bpm) OVER (ORDER BY bpm, id) AS bpm_change
    FROM with_details_artist
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_country_artist_id_by AS (PARTITION BY artist_country, artist_id ORDER BY bpm ASC)
),
top_per_group AS (
    -- Top 3 per group
    SELECT
        id,
        artist_id,
        title,
        bpm,
        artist_name,
        artist_country,
        artist_id_avg_bpm,
        bpm_percent_rank,
        second_bpm,
        bpm_dense_rank,
        rows_in_artist_id,
        bpm_cume_dist,
        bpm_percent_rank_2,
        bpm_decile,
        bpm_quintile,
        bpm_quartile,
        bpm_change
    FROM windowed
    WHERE bpm_dense_rank_2 <= 3
      AND bpm_decile > 4
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SIZE(COLLECT_SET(artist_id) OVER w_artist_country_bpm_decile) AS distinct_artist_id_in_artist_country_bpm_decile,
        NTH_VALUE(bpm_dense_rank, 2) OVER (PARTITION BY bpm_quartile ORDER BY bpm_change DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm_dense_rank,
        COUNT(*) OVER (PARTITION BY artist_country, bpm_decile) AS rows_in_artist_country_bpm_decile,
        bpm_percent_rank - LAG(bpm_percent_rank) OVER (PARTITION BY bpm_quartile ORDER BY bpm_percent_rank DESC, id) AS bpm_percent_rank_change,
        LAG(bpm, 3) OVER (PARTITION BY bpm_quartile ORDER BY bpm_percent_rank DESC, id) AS prev3_bpm
    FROM top_per_group
    WINDOW w_artist_country_bpm_decile AS (PARTITION BY artist_country, bpm_decile)
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    bpm,
    artist_name,
    artist_country,
    artist_id_avg_bpm,
    bpm_percent_rank,
    second_bpm,
    bpm_dense_rank,
    rows_in_artist_id,
    bpm_cume_dist,
    bpm_percent_rank_2,
    bpm_quintile,
    distinct_artist_id_in_artist_country_bpm_decile,
    rows_in_artist_country_bpm_decile,
    prev3_bpm
FROM ranked
