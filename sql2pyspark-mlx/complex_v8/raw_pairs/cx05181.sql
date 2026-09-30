WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        release_date,
        is_explicit,
        bpm,
        moods,
        INITCAP(TRIM(title)) AS title_clean
    FROM tracks
    WHERE (UPPER(TRIM(title)) LIKE 'C%' OR artist_id <= 6)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY bpm) AS bpm_rank,
        LAG(release_date) OVER w_release_date_month_by_releas AS prev_release_date,
        LAST_VALUE(release_date) IGNORE NULLS OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_release_date,
        ROUND(bpm - AVG(bpm) OVER (PARTITION BY is_explicit), 2) AS bpm_vs_is_explicit_avg,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC, id)) AS days_since_prev_release_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY bpm ASC), 3) AS bpm_percent_rank,
        NTILE(10) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC, id) AS release_date_decile,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        NTH_VALUE(bpm, 2) OVER (ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_bpm,
        DATEDIFF(release_date, LAG(release_date) OVER (PARTITION BY artist_id ORDER BY release_date DESC, id)) AS days_since_prev_release_date_2,
        RANK() OVER (PARTITION BY artist_id ORDER BY release_date DESC) AS release_date_rank,
        ROW_NUMBER() OVER w_is_explicit_by_bpm AS seq_no,
        DATEDIFF(release_date, LAG(release_date) OVER (ORDER BY bpm, id)) AS days_since_prev_release_date_3
    FROM base
    WINDOW w_release_date_month_by_releas AS (PARTITION BY DATE_TRUNC('MONTH', release_date) ORDER BY release_date DESC, id), w_is_explicit_by_bpm AS (PARTITION BY is_explicit ORDER BY bpm ASC, id)
),
first_rows AS (
    -- Top 2 per group
    SELECT *
    FROM sequenced
    WHERE seq_no <= 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(days_since_prev_release_date) OVER (PARTITION BY seq_no, YEAR(prev_release_date) ORDER BY days_since_prev_release_date_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_days_since_prev_release_date,
        MIN(release_date_rank) OVER w_prev_release_date_month AS prev_release_date_month_min_release_date_rank,
        SUM(days_since_prev_release_date) OVER (PARTITION BY release_date_rank ORDER BY bpm_rank ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS days_since_prev_release_date_sum_7,
        ROUND(AVG(bpm) OVER (PARTITION BY release_date_rank ORDER BY bpm_rank ASC, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS bpm_avg_6_prior,
        MIN(bpm) OVER (PARTITION BY seq_no, YEAR(prev_release_date) ORDER BY days_since_prev_release_date_2 ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_bpm_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY seq_no, YEAR(prev_release_date) ORDER BY days_since_prev_release_date_2 ASC, id), 3) AS days_since_prev_release_date_2_cume_dist,
        FIRST_VALUE(prev_release_date) OVER (PARTITION BY release_date_rank ORDER BY bpm_rank ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_prev_release_date
    FROM first_rows
    WINDOW w_prev_release_date_month AS (PARTITION BY DATE_TRUNC('MONTH', prev_release_date))
)
-- Final projection
SELECT
    id,
    artist_id,
    title,
    release_date,
    bpm,
    moods,
    title_clean,
    bpm_rank,
    prev_release_date,
    last_known_release_date,
    bpm_vs_is_explicit_avg,
    bpm_percent_rank,
    release_date_decile,
    rows_in_is_explicit,
    second_bpm,
    release_date_rank,
    seq_no,
    last_days_since_prev_release_date,
    prev_release_date_month_min_release_date_rank,
    days_since_prev_release_date_sum_7,
    days_since_prev_release_date_2_cume_dist,
    first_prev_release_date
FROM metrics
ORDER BY days_since_prev_release_date_sum_7 NULLS LAST, bpm_rank DESC, id
LIMIT 10
