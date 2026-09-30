WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        SUBSTRING(TRIM(title), 1, 2) AS title_prefix
    FROM tracks
    WHERE is_explicit = TRUE
),
with_details AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.duration_sec,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.moods,
        s.title_prefix,
        a.country AS artist_country,
        a.debut_year AS artist_debut_year
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id AND UPPER(TRIM(a.artist_name)) LIKE 'K%'
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        title_prefix,
        artist_country,
        artist_debut_year,
        LAST_VALUE(artist_debut_year) IGNORE NULLS OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_artist_debut_year,
        MIN(duration_sec) OVER (PARTITION BY artist_id) AS artist_id_min_duration_sec,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_share_of_artist_id_is_explicit,
        ROW_NUMBER() OVER (PARTITION BY artist_country ORDER BY release_date DESC, id) AS seq_no,
        COUNT(*) OVER w_artist_id AS rows_in_artist_id,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_id), 2) AS artist_id_avg_bpm,
        NTILE(4) OVER w_artist_country_by_release_da AS release_date_quartile,
        ROUND(bpm - AVG(bpm) OVER w_artist_id_is_explicit, 2) AS bpm_vs_artist_id_is_explicit_avg,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY artist_id), 2) AS duration_sec_vs_artist_id_avg,
        RANK() OVER w_artist_country_artist_id_by AS bpm_rank,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY artist_debut_year DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec,
        MAX(bpm) OVER (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        SUM(duration_sec) OVER (PARTITION BY artist_country ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        LAG(bpm) OVER w_release_date_year_artist_id_2 AS prev_bpm
    FROM with_details
    WINDOW w_artist_id AS (PARTITION BY artist_id), w_artist_country_by_release_da AS (PARTITION BY artist_country ORDER BY release_date DESC, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit), w_artist_country_artist_id_by AS (PARTITION BY artist_country, artist_id ORDER BY bpm NULLS LAST), w_release_date_year_artist_id_2 AS (PARTITION BY YEAR(release_date), artist_id ORDER BY duration_sec, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(last_known_artist_debut_year - AVG(last_known_artist_debut_year) OVER (PARTITION BY release_date_quartile), 2) AS last_known_artist_debut_year_vs_release_date_qua,
        LAG(seq_no, 1, 0) OVER (PARTITION BY release_date_quartile ORDER BY seq_no ASC, id) AS prev_seq_no,
        ROUND(bpm_rank / NULLIF(SUM(bpm_rank) OVER (PARTITION BY artist_country, artist_id), 0), 4) AS bpm_rank_share_of_artist_country_artist_id,
        SUM(rows_in_artist_id) OVER (PARTITION BY release_date_quartile, seq_no ORDER BY duration_sec_vs_artist_id_avg DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rows_in_artist_id_sum_6_prior,
        ROUND(artist_id_min_duration_sec / NULLIF(SUM(artist_id_min_duration_sec) OVER w_artist_country_artist_id, 0), 4) AS artist_id_min_duration_sec_share_of_artist_count
    FROM with_windows
    WINDOW w_artist_country_artist_id AS (PARTITION BY artist_country, artist_id)
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    bpm,
    artist_debut_year,
    artist_id_min_duration_sec,
    bpm_vs_artist_id_avg,
    seq_no,
    rows_in_artist_id,
    artist_id_avg_bpm,
    bpm_vs_artist_id_is_explicit_avg,
    bpm_rank,
    first_known_duration_sec,
    max_bpm_so_far,
    running_duration_sec,
    prev_bpm,
    last_known_artist_debut_year_vs_release_date_qua,
    prev_seq_no,
    bpm_rank_share_of_artist_country_artist_id,
    rows_in_artist_id_sum_6_prior,
    artist_id_min_duration_sec_share_of_artist_count,
    CONCAT_WS('-', artist_country, title) AS artist_country_title_label
FROM sequenced
