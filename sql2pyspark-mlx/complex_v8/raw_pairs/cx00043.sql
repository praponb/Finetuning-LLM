WITH filtered AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        CONCAT(title, '#', CAST(bpm AS STRING)) AS title_ref
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'M%'
      AND artist_id >= 14
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        moods,
        title_ref,
        ROUND(CUME_DIST() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id), 3) AS release_date_cume_dist,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        COUNT(*) OVER (PARTITION BY artist_id) AS rows_in_artist_id,
        DATEDIFF(release_date, LAG(release_date) OVER w_is_explicit_by_release_date) AS days_since_prev_release_date,
        ROUND(CUME_DIST() OVER (ORDER BY bpm, id), 3) AS bpm_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id) AS seq_no,
        ROW_NUMBER() OVER (PARTITION BY is_explicit ORDER BY release_date DESC, id) AS rn,
        SUM(bpm) OVER (PARTITION BY artist_id) AS artist_id_sum_bpm,
        LEAD(bpm, 1, 0) OVER (PARTITION BY artist_id ORDER BY release_date NULLS LAST, id) AS next_bpm,
        ROW_NUMBER() OVER w_all_by_release_date AS rn_2,
        MAX(bpm) OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_bpm_so_far,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id), 2) AS bpm_vs_artist_id_avg
    FROM filtered
    WINDOW w_is_explicit_by_release_date AS (PARTITION BY is_explicit ORDER BY release_date DESC, id), w_all_by_release_date AS (ORDER BY release_date ASC, id)
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    moods,
    title_ref,
    release_date_cume_dist,
    first_known_release_date,
    rows_in_artist_id,
    days_since_prev_release_date,
    bpm_cume_dist,
    seq_no,
    rn,
    artist_id_sum_bpm,
    next_bpm,
    max_bpm_so_far,
    bpm_vs_artist_id_avg,
    UPPER(TRIM(title)) AS title_clean,
    DATE_FORMAT(release_date, 'MMM yyyy') AS release_date_label
FROM sequenced
ORDER BY title, id
LIMIT 20
