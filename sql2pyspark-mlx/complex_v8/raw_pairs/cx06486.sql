WITH cleaned_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        release_date,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        LEAST(duration_sec, bpm) AS least_duration_sec_bpm
    FROM tracks
    WHERE SIZE(moods) >= 2
      AND duration_sec >= 154
),
joined AS (
    -- Join artists
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.bpm,
        s.moods,
        s.least_duration_sec_bpm,
        a.genre AS artist_genre,
        a.is_verified AS artist_is_verified
    FROM cleaned_tracks s
    JOIN artists a ON a.id = s.artist_id
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER w_all_by_release_date AS release_date_rank,
        RANK() OVER (ORDER BY release_date) AS release_date_rank_2,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_is_verified) AS rows_in_release_date_month_artist_is_verified,
        SUM(least_duration_sec_bpm) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_least_duration_sec_bpm,
        MIN(least_duration_sec_bpm) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_is_verified) AS release_date_month_artist_is_verified_min_least_,
        RANK() OVER (PARTITION BY artist_is_verified ORDER BY least_duration_sec_bpm, id) AS least_duration_sec_bpm_rank,
        LAST_VALUE(release_date) OVER (PARTITION BY artist_is_verified ORDER BY least_duration_sec_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date,
        LAST_VALUE(bpm) OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_bpm,
        NTH_VALUE(bpm, 2) OVER (PARTITION BY artist_is_verified ORDER BY least_duration_sec_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        MAX(bpm) OVER (PARTITION BY artist_is_verified, artist_id ORDER BY least_duration_sec_bpm ASC NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS bpm_max_1_prior,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY artist_is_verified, artist_id ORDER BY least_duration_sec_bpm ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        ROW_NUMBER() OVER (PARTITION BY artist_is_verified ORDER BY least_duration_sec_bpm, id) AS rn,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date
    FROM joined
    WINDOW w_all_by_release_date AS (ORDER BY release_date, id)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(second_bpm) OVER (PARTITION BY artist_genre, rn) AS artist_genre_rn_sum_second_bpm,
        LEAD(release_date) OVER (PARTITION BY release_date_rank_2 ORDER BY running_least_duration_sec_bpm, id) AS next_release_date,
        DATEDIFF(last_release_date, LAG(last_release_date) OVER (PARTITION BY release_date_rank, least_duration_sec_bpm_rank ORDER BY last_release_date, id)) AS days_since_prev_last_release_date,
        NTH_VALUE(release_date_rank_2, 2) OVER (PARTITION BY release_date_rank_2 ORDER BY running_least_duration_sec_bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date_rank_2,
        ROUND(least_duration_sec_bpm / NULLIF(SUM(least_duration_sec_bpm) OVER (PARTITION BY rn), 0), 4) AS least_duration_sec_bpm_share_of_rn,
        LAG(bpm_max_1_prior, 1, 0) OVER (ORDER BY rn, id) AS prev_bpm_max_1_prior,
        COUNT(*) OVER w_artist_genre_rn AS rows_in_artist_genre_rn
    FROM with_windows
    WINDOW w_artist_genre_rn AS (PARTITION BY artist_genre, rn)
)
SELECT
    id,
    artist_id,
    release_date,
    bpm,
    least_duration_sec_bpm,
    artist_genre,
    release_date_rank,
    release_date_rank_2,
    rows_in_release_date_month_artist_is_verified,
    running_least_duration_sec_bpm,
    release_date_month_artist_is_verified_min_least_,
    least_duration_sec_bpm_rank,
    second_bpm,
    bpm_max_1_prior,
    last_known_release_date,
    next_release_date,
    second_release_date_rank_2,
    least_duration_sec_bpm_share_of_rn,
    prev_bpm_max_1_prior,
    rows_in_artist_genre_rn
FROM windowed
WHERE days_since_prev_last_release_date < 170
ORDER BY rows_in_release_date_month_artist_is_verified DESC, bpm_max_1_prior DESC
