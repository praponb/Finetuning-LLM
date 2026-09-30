WITH valid AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        DATE_TRUNC('WEEK', release_date) AS release_date_week
    FROM tracks
),
unique_rows AS (
    -- Keep the latest row per artist_id
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        release_date_week
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY artist_id ORDER BY release_date DESC, id DESC) AS row_num
        FROM valid
    ) ranked
    WHERE row_num = 1
),
play_stats AS (
    -- Per-track plays metrics
    SELECT track_id, MAX(played_at) AS last_played_at
    FROM plays
    GROUP BY track_id
),
summarized_rows AS (
    -- Attach plays metrics
    SELECT
        s.*,
        p.last_played_at
    FROM unique_rows s
    LEFT JOIN play_stats p ON p.track_id = s.id
),
per_group AS (
    -- Group-level totals
    SELECT
        TO_DATE(release_date_week) AS release_date_week_day,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
        SUM(duration_sec) AS total_duration_sec
    FROM summarized_rows
    GROUP BY TO_DATE(release_date_week), DATE_FORMAT(release_date, 'yyyy-MM')
),
screened AS (
    -- Compare against the overall max
    SELECT *
    FROM per_group
    WHERE avg_duration_sec < (SELECT MAX(avg_duration_sec) FROM per_group)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_release_date_week_day_month AS rows_in_release_date_week_day_month,
        LEAD(avg_duration_sec, 2) OVER (ORDER BY total_duration_sec DESC, release_date_week_day, release_date_ym) AS next2_avg_duration_sec,
        MAX(total_duration_sec) OVER (PARTITION BY MONTH(release_date_week_day)) AS release_date_week_day_month_max_total_duration_s,
        ROW_NUMBER() OVER (ORDER BY total_duration_sec DESC, release_date_week_day, release_date_ym) AS total_duration_sec_seq,
        MAX(total_duration_sec) OVER (ORDER BY total_duration_sec DESC, release_date_week_day, release_date_ym ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS total_duration_sec_max_3_prior,
        DENSE_RANK() OVER w_all_by_total_duration_sec_ti AS total_duration_sec_dense_rank
    FROM screened
    WINDOW w_release_date_week_day_month AS (PARTITION BY MONTH(release_date_week_day)), w_all_by_total_duration_sec_ti AS (ORDER BY total_duration_sec DESC)
),
derived AS (
    -- Add calculated columns
    SELECT
        *,
        DAYOFWEEK(release_date_week_day) AS release_date_week_day_dayofweek,
        ROUND(total_duration_sec / NULLIF(total_duration_sec_seq, 0), 3) AS total_duration_sec_per_total_duration_sec_seq,
        DATEDIFF(DATE '2025-12-31', release_date_week_day) AS days_since_release_date_week_day
    FROM sequenced
)
-- Final projection
SELECT
    release_date_week_day,
    release_date_ym,
    avg_duration_sec,
    total_duration_sec,
    next2_avg_duration_sec,
    total_duration_sec_per_total_duration_sec_seq,
    LENGTH(TRIM(release_date_ym)) AS release_date_ym_len
FROM derived
ORDER BY release_date_week_day DESC, release_date_ym
LIMIT 10
