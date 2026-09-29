WITH cleaned AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        duration_sec - bpm AS duration_sec_minus_bpm
    FROM tracks
    WHERE SIZE(moods) >= 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        artist_id,
        title,
        release_date,
        bpm,
        COUNT(*) OVER (PARTITION BY artist_id, is_explicit) AS rows_in_artist_id_is_explicit,
        DENSE_RANK() OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec_minus_bpm DESC) AS duration_sec_minus_bpm_dense_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS bpm_share_of_artist_id_is_explicit,
        DENSE_RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm) AS duration_sec_minus_bpm_dense_rank_2,
        ROW_NUMBER() OVER w_artist_id_by_duration_sec_mi_2 AS seq_no,
        ROUND(duration_sec_minus_bpm / NULLIF(SUM(duration_sec_minus_bpm) OVER (PARTITION BY artist_id, is_explicit), 0), 4) AS duration_sec_minus_bpm_share_of_artist_id_is_exp,
        FIRST_VALUE(release_date) OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        ROUND(bpm - AVG(bpm) OVER w_artist_id_is_explicit, 2) AS bpm_vs_artist_id_is_explicit_avg,
        NTH_VALUE(release_date, 2) OVER (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        LEAD(release_date) OVER (PARTITION BY is_explicit, DATE_TRUNC('MONTH', release_date) ORDER BY duration_sec_minus_bpm DESC, id) AS next_release_date
    FROM cleaned
    WINDOW w_artist_id_by_duration_sec_mi_2 AS (PARTITION BY artist_id ORDER BY duration_sec_minus_bpm, id), w_artist_id_is_explicit AS (PARTITION BY artist_id, is_explicit)
),
derived AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(title)) AS title_len,
        DATEDIFF(DATE '2025-06-30', next_release_date) AS days_since_next_release_date,
        CASE WHEN bpm >= 127 THEN 'A' WHEN bpm >= 89 THEN 'B' ELSE 'C' END AS bpm_tier
    FROM metrics
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        RANK() OVER w_all_by_duration_sec_minus_bp AS duration_sec_minus_bpm_share_of_artist_id_is_exp_2,
        NTH_VALUE(days_since_next_release_date, 2) OVER (PARTITION BY bpm_tier, DATE_TRUNC('MONTH', next_release_date) ORDER BY second_release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_days_since_next_release_date,
        LAST_VALUE(first_release_date) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), seq_no ORDER BY bpm_share_of_artist_id_is_explicit ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_first_release_date,
        bpm - LAG(bpm) OVER (PARTITION BY bpm_tier, DATE_TRUNC('MONTH', next_release_date) ORDER BY second_release_date, id) AS bpm_change
    FROM derived
    WINDOW w_all_by_duration_sec_minus_bp AS (ORDER BY duration_sec_minus_bpm_share_of_artist_id_is_exp)
)
SELECT
    id,
    artist_id,
    title,
    release_date,
    bpm,
    rows_in_artist_id_is_explicit,
    bpm_share_of_artist_id_is_explicit,
    duration_sec_minus_bpm_dense_rank_2,
    seq_no,
    duration_sec_minus_bpm_share_of_artist_id_is_exp,
    bpm_vs_artist_id_is_explicit_avg,
    second_release_date,
    title_len,
    days_since_next_release_date,
    bpm_tier,
    second_days_since_next_release_date,
    last_first_release_date,
    bpm_change
FROM with_trends
ORDER BY second_release_date, bpm_change DESC NULLS FIRST
