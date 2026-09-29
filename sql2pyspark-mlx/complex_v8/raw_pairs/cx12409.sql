WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        MONTH(release_date) AS release_date_month
    FROM tracks
    WHERE duration_sec BETWEEN 183 AND 352
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        moods,
        release_date_month,
        ROUND(AVG(release_date_month) OVER (ORDER BY release_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS release_date_month_avg_2_prior,
        MAX(bpm) OVER (PARTITION BY artist_id, is_explicit) AS artist_id_is_explicit_max_bpm,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY artist_id, is_explicit), 2) AS bpm_vs_artist_id_is_explicit_avg,
        FIRST_VALUE(release_date_month) IGNORE NULLS OVER (ORDER BY release_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date_month
    FROM scoped
),
with_children_plays AS (
    -- One row per play
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.moods,
        s.release_date_month,
        s.release_date_month_avg_2_prior,
        s.artist_id_is_explicit_max_bpm,
        s.bpm_vs_artist_id_is_explicit_avg,
        s.first_known_release_date_month,
        p.id AS play_id,
        p.listener_id AS play_listener_id,
        p.ms_played AS play_ms_played,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at
    FROM windowed s
    JOIN plays p ON p.track_id = s.id
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(play_played_at) AS play_played_at_year,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        ROUND(AVG(play_ms_played), 2) AS avg_play_ms_played,
        DATEDIFF(MAX(release_date), MIN(release_date)) AS release_date_span_days
    FROM with_children_plays
    GROUP BY YEAR(play_played_at), DATE_FORMAT(release_date, 'yyyy-MM')
),
windowed_2 AS (
    -- Previous / next values per key
    SELECT
        *,
        LAST_VALUE(release_date_span_days) OVER (ORDER BY avg_play_ms_played DESC, play_played_at_year, release_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_release_date_span_days,
        ROUND(AVG(play_played_at_year) OVER (PARTITION BY play_played_at_year), 2) AS play_played_at_year_avg_play_played_at_year,
        MAX(play_played_at_year) OVER (ORDER BY avg_play_ms_played DESC, play_played_at_year, release_date_ym ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS play_played_at_year_max_4,
        ROUND(release_date_span_days - AVG(release_date_span_days) OVER (PARTITION BY play_played_at_year), 2) AS release_date_span_days_vs_play_played_at_year_av,
        MAX(avg_play_ms_played) OVER (ORDER BY avg_play_ms_played DESC, play_played_at_year, release_date_ym ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS avg_play_ms_played_max_4
    FROM summary
),
metrics AS (
    -- Previous / next values per key
    SELECT
        play_played_at_year,
        release_date_ym,
        avg_play_ms_played,
        release_date_span_days,
        last_release_date_span_days,
        play_played_at_year_avg_play_played_at_year,
        play_played_at_year_max_4,
        release_date_span_days_vs_play_played_at_year_av,
        avg_play_ms_played_max_4,
        SUM(avg_play_ms_played) OVER (PARTITION BY play_played_at_year) AS play_played_at_year_sum_avg_play_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY play_played_at_year ORDER BY release_date_span_days_vs_play_played_at_year_av), 3) AS release_date_span_days_vs_play_played_at_year_av_2,
        MIN(play_played_at_year_max_4) OVER (PARTITION BY play_played_at_year ORDER BY release_date_span_days_vs_play_played_at_year_av, play_played_at_year, release_date_ym ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_play_played_at_year_max_4_so_far,
        ROUND(avg_play_ms_played_max_4 / NULLIF(SUM(avg_play_ms_played_max_4) OVER (PARTITION BY play_played_at_year), 0), 4) AS avg_play_ms_played_max_4_share_of_play_played_at,
        COUNT(*) OVER (PARTITION BY play_played_at_year) AS rows_in_play_played_at_year
    FROM windowed_2
)
SELECT play_played_at_year, release_date_ym, metric, value
FROM (
    SELECT play_played_at_year, release_date_ym, release_date_span_days_vs_play_played_at_year_av_2, release_date_span_days_vs_play_played_at_year_av, play_played_at_year_sum_avg_play_ms_played, avg_play_ms_played
    FROM metrics
) src
UNPIVOT (
    value FOR metric IN (release_date_span_days_vs_play_played_at_year_av_2, release_date_span_days_vs_play_played_at_year_av, play_played_at_year_sum_avg_play_ms_played, avg_play_ms_played)
) unp
