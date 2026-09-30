WITH base AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods
    FROM tracks
    WHERE release_date < DATE '2024-12-22'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTH_VALUE(duration_sec, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_duration_sec,
        LAG(bpm) OVER (PARTITION BY MONTH(release_date), is_explicit ORDER BY release_date ASC, id) AS prev_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit, artist_id), 2) AS bpm_vs_is_explicit_artist_id_avg,
        SUM(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROUND(duration_sec - AVG(duration_sec) OVER (PARTITION BY is_explicit), 2) AS duration_sec_vs_is_explicit_avg,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit), 0), 4) AS duration_sec_share_of_is_explicit,
        NTH_VALUE(release_date, 2) OVER (ORDER BY duration_sec ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        MAX(duration_sec) OVER (PARTITION BY MONTH(release_date), is_explicit ORDER BY release_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS duration_sec_max_4,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_id, is_explicit ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        SUM(duration_sec) OVER w_is_explicit_artist_id AS is_explicit_artist_id_sum_duration_sec,
        NTILE(4) OVER (ORDER BY duration_sec ASC, id) AS duration_sec_quartile,
        FIRST_VALUE(bpm) IGNORE NULLS OVER (PARTITION BY artist_id, is_explicit ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_bpm
    FROM base
    WINDOW w_is_explicit_artist_id AS (PARTITION BY is_explicit, artist_id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        FLOOR(bpm / 25) * 25 AS bpm_bucket,
        ROUND(duration_sec / NULLIF(duration_sec_max_4, 0), 3) AS duration_sec_per_duration_sec_max_4,
        FLOOR(first_known_bpm / 25) * 25 AS first_known_bpm_bucket
    FROM windowed
    WHERE second_duration_sec BETWEEN 165 AND 365
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(CUME_DIST() OVER w_artist_id_by_release_date_ti, 3) AS release_date_cume_dist,
        ROUND(second_duration_sec / NULLIF(SUM(second_duration_sec) OVER w_duration_sec_quartile_second, 0), 4) AS second_duration_sec_share_of_duration_sec_quarti,
        DENSE_RANK() OVER w_artist_id_by_release_date AS release_date_dense_rank,
        ROUND(PERCENT_RANK() OVER w_duration_sec_quartile_by_bpm, 3) AS bpm_bucket_percent_rank,
        ROUND(bpm / NULLIF(SUM(bpm) OVER (PARTITION BY duration_sec_quartile, MONTH(second_release_date)), 0), 4) AS bpm_share_of_duration_sec_quartile_second_releas,
        NTH_VALUE(second_duration_sec, 2) OVER (PARTITION BY duration_sec_quartile ORDER BY bpm_bucket DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_second_duration_sec
    FROM scored
    WINDOW w_artist_id_by_release_date_ti AS (PARTITION BY artist_id ORDER BY release_date DESC NULLS FIRST), w_duration_sec_quartile_second AS (PARTITION BY duration_sec_quartile, MONTH(second_release_date)), w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date DESC NULLS FIRST, id), w_duration_sec_quartile_by_bpm AS (PARTITION BY duration_sec_quartile ORDER BY bpm_bucket DESC)
)
SELECT
    id,
    artist_id,
    duration_sec,
    release_date,
    is_explicit,
    second_duration_sec,
    prev_bpm,
    bpm_vs_is_explicit_artist_id_avg,
    duration_sec_vs_is_explicit_avg,
    duration_sec_share_of_is_explicit,
    second_release_date,
    duration_sec_max_4,
    second_bpm,
    is_explicit_artist_id_sum_duration_sec,
    duration_sec_quartile,
    release_date_cume_dist,
    second_duration_sec_share_of_duration_sec_quarti,
    bpm_bucket_percent_rank,
    bpm_share_of_duration_sec_quartile_second_releas,
    second_second_duration_sec
FROM ranked
ORDER BY duration_sec_max_4 NULLS LAST
