WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM-dd') AS release_date_label,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE artist_id >= 6
),
with_details_artist AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_ref,
        a.debut_year AS artist_debut_year,
        a.is_verified AS artist_is_verified,
        a.artist_name
    FROM valid_tracks s
    LEFT JOIN artists a ON a.id = s.artist_id AND a.is_verified = TRUE
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        title_ref,
        artist_debut_year,
        artist_is_verified,
        artist_name,
        RANK() OVER (PARTITION BY artist_is_verified ORDER BY duration_sec) AS duration_sec_rank,
        MAX(bpm) OVER w_artist_id_is_explicit AS artist_id_is_explicit_max_bpm,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY artist_debut_year ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        COUNT(*) OVER (PARTITION BY artist_id, artist_is_verified) AS rows_in_artist_id_artist_is_verified,
        COUNT(*) OVER w_artist_id_is_explicit AS rows_in_artist_id_is_explicit,
        DENSE_RANK() OVER w_artist_is_verified_release_d AS bpm_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY artist_is_verified ORDER BY duration_sec), 3) AS duration_sec_percent_rank,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        LEAD(duration_sec) OVER (PARTITION BY artist_is_verified, DATE_TRUNC('MONTH', release_date) ORDER BY bpm ASC, id) AS next_duration_sec,
        COUNT(*) OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY duration_sec DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count,
        LEAD(bpm, 2) OVER w_artist_is_verified_release_d AS next2_bpm,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_is_verified, DATE_TRUNC('MONTH', release_date) ORDER BY bpm ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, artist_is_verified), 2) AS bpm_vs_artist_id_artist_is_verified_avg,
        RANK() OVER (PARTITION BY is_explicit, artist_is_verified ORDER BY artist_debut_year ASC) AS artist_debut_year_rank
    FROM with_details_artist
    WINDOW w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_artist_is_verified_release_d AS (PARTITION BY artist_is_verified, DATE_TRUNC('MONTH', release_date) ORDER BY bpm ASC, id)
),
first_rows AS (
    -- First row per group
    SELECT *
    FROM windowed
    WHERE bpm_dense_rank = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        next2_bpm - LAG(next2_bpm) OVER (PARTITION BY artist_id ORDER BY next2_bpm ASC, id) AS next2_bpm_change,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY next2_bpm ASC, id), 3) AS next2_bpm_cume_dist,
        ROUND(artist_debut_year_rank / NULLIF(SUM(artist_debut_year_rank) OVER (PARTITION BY artist_is_verified, MONTH(first_known_release_date)), 0), 4) AS artist_debut_year_rank_share_of_artist_is_verifi,
        ROW_NUMBER() OVER (PARTITION BY MONTH(release_date), duration_sec_rank ORDER BY running_count, id) AS running_count_seq,
        ROUND(PERCENT_RANK() OVER w_artist_id_by_next2_bpm, 3) AS next2_bpm_percent_rank,
        ROUND(artist_debut_year_rank / NULLIF(SUM(artist_debut_year_rank) OVER w_bpm_dense_rank, 0), 4) AS artist_debut_year_rank_share_of_bpm_dense_rank,
        ROUND(AVG(duration_sec_percent_rank) OVER (PARTITION BY artist_is_verified, MONTH(first_known_release_date)), 2) AS artist_is_verified_first_known_release_date_mont
    FROM first_rows
    WINDOW w_artist_id_by_next2_bpm AS (PARTITION BY artist_id ORDER BY next2_bpm ASC, id), w_bpm_dense_rank AS (PARTITION BY bpm_dense_rank)
)
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    moods,
    title_ref,
    artist_debut_year,
    artist_is_verified,
    duration_sec_rank,
    artist_id_is_explicit_max_bpm,
    first_known_release_date,
    rows_in_artist_id_artist_is_verified,
    bpm_dense_rank,
    next2_bpm,
    first_known_bpm,
    first_known_duration_sec,
    bpm_vs_artist_id_artist_is_verified_avg,
    artist_debut_year_rank,
    next2_bpm_change,
    artist_debut_year_rank_share_of_artist_is_verifi,
    next2_bpm_percent_rank,
    artist_debut_year_rank_share_of_bpm_dense_rank,
    artist_is_verified_first_known_release_date_mont,
    CASE WHEN artist_debut_year_rank >= 7 THEN 'large' WHEN artist_debut_year_rank >= 4 THEN 'medium' ELSE 'small' END AS artist_debut_year_rank_tier
FROM metrics
ORDER BY rows_in_artist_id_artist_is_verified
