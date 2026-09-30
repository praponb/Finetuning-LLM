WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm
    FROM tracks
    WHERE bpm <= 142
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        title,
        release_date,
        is_explicit,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)), 0), 4) AS bpm_share_of_artist_id_release_date_month,
        ROW_NUMBER() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS bpm_seq,
        MAX(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS bpm_max_6_prior,
        RANK() OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS bpm_rank,
        MAX(bpm) OVER (PARTITION BY artist_id, DATE_TRUNC('MONTH', release_date)) AS artist_id_release_date_month_max_bpm,
        LAG(bpm) OVER (PARTITION BY is_explicit, artist_id ORDER BY bpm DESC, id) AS prev_bpm
    FROM recent
)
-- Subtotals with GROUPING SETS
SELECT
    is_explicit,
    bpm_seq,
    bpm_rank,
    MIN(bpm_max_6_prior) AS min_bpm_max_6_prior
FROM with_trends
GROUP BY GROUPING SETS ((is_explicit, bpm_seq, bpm_rank), (is_explicit), (bpm_seq), ())
ORDER BY is_explicit NULLS LAST, bpm_seq NULLS LAST, bpm_rank NULLS LAST
