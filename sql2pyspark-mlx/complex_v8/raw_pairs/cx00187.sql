WITH prepared_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        is_explicit,
        bpm,
        moods,
        ARRAY_CONTAINS(moods, 'party') AS has_party
    FROM tracks
    WHERE duration_sec <= 365
      AND artist_id IS NOT NULL
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        is_explicit,
        moods,
        has_party,
        ROUND(CUME_DIST() OVER w_all_by_bpm_ties, 3) AS bpm_cume_dist,
        COUNT(*) OVER w_is_explicit_has_party AS rows_in_is_explicit_has_party,
        ROUND(PERCENT_RANK() OVER w_all_by_bpm_ties_2, 3) AS bpm_percent_rank,
        RANK() OVER (ORDER BY bpm NULLS LAST) AS bpm_rank,
        SUM(bpm) OVER (ORDER BY bpm DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm,
        LAG(bpm, 3) OVER (ORDER BY bpm NULLS LAST, id) AS prev3_bpm,
        SUM(bpm) OVER (PARTITION BY has_party, is_explicit ORDER BY bpm, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS bpm_sum_4,
        ROUND(PERCENT_RANK() OVER (PARTITION BY has_party, is_explicit ORDER BY bpm), 3) AS bpm_percent_rank_2,
        MAX(bpm) OVER (PARTITION BY is_explicit, artist_id) AS is_explicit_artist_id_max_bpm,
        LEAD(bpm) OVER w_all_by_bpm_2 AS next_bpm,
        NTILE(10) OVER w_has_party_is_explicit_by_bpm_3 AS bpm_decile,
        SUM(bpm) OVER (ORDER BY bpm NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_bpm_2,
        LAG(bpm, 2) OVER (ORDER BY bpm DESC NULLS FIRST, id) AS prev2_bpm
    FROM prepared_tracks
    WINDOW w_all_by_bpm_ties AS (ORDER BY bpm NULLS LAST), w_is_explicit_has_party AS (PARTITION BY is_explicit, has_party), w_all_by_bpm_ties_2 AS (ORDER BY bpm), w_all_by_bpm_2 AS (ORDER BY bpm DESC NULLS FIRST, id), w_has_party_is_explicit_by_bpm_3 AS (PARTITION BY has_party, is_explicit ORDER BY bpm, id)
)
-- Final projection
SELECT
    id,
    artist_id,
    is_explicit,
    moods,
    bpm_cume_dist,
    rows_in_is_explicit_has_party,
    bpm_percent_rank,
    bpm_sum_4,
    bpm_percent_rank_2,
    next_bpm,
    bpm_decile,
    running_bpm_2,
    prev2_bpm,
    ARRAY_CONTAINS(moods, 'sad') AS has_sad,
    CASE WHEN is_explicit THEN 1 ELSE 0 END AS is_explicit_flag
FROM windowed
WHERE bpm_percent_rank <= 0.8
ORDER BY running_bpm_2, bpm_decile DESC NULLS FIRST, id
LIMIT 10
