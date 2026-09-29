WITH base AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        release_date,
        is_explicit,
        bpm,
        IF(is_explicit, 'yes', 'no') AS is_explicit_label,
        DAYOFWEEK(release_date) AS release_date_dayofweek
    FROM tracks
    WHERE bpm >= 82
),
combined_details_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.release_date,
        s.is_explicit,
        s.bpm,
        s.is_explicit_label,
        a.country AS artist_country
    FROM base s
    JOIN artists a ON a.id = s.artist_id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        release_date,
        is_explicit,
        bpm,
        is_explicit_label,
        COUNT(*) OVER w_is_explicit_is_explicit_labe AS rows_in_is_explicit_is_explicit_label,
        ROUND(bpm / NULLIF(SUM(bpm) OVER w_release_date_month_is_explic, 0), 4) AS bpm_share_of_release_date_month_is_explicit,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY bpm DESC, id), 3) AS bpm_percent_rank,
        SIZE(COLLECT_SET(artist_country) OVER w_release_date_month_is_explic) AS distinct_artist_country_in_release_date_month_is,
        FIRST_VALUE(bpm) OVER (PARTITION BY artist_country ORDER BY bpm, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_bpm,
        bpm - LAG(bpm) OVER w_artist_country_by_bpm AS bpm_change,
        ROUND(AVG(bpm) OVER (PARTITION BY artist_country, artist_id ORDER BY bpm ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS bpm_avg_7,
        RANK() OVER (ORDER BY release_date, id) AS release_date_rank,
        MAX(bpm) OVER (PARTITION BY MONTH(release_date), is_explicit) AS release_date_month_is_explicit_max_bpm,
        MIN(bpm) OVER (PARTITION BY MONTH(release_date), is_explicit) AS release_date_month_is_explicit_min_bpm,
        ROUND(CUME_DIST() OVER (ORDER BY release_date), 3) AS release_date_cume_dist
    FROM combined_details_artist
    WINDOW w_is_explicit_is_explicit_labe AS (PARTITION BY is_explicit, is_explicit_label), w_release_date_month_is_explic AS (PARTITION BY MONTH(release_date), is_explicit), w_artist_country_by_bpm AS (PARTITION BY artist_country ORDER BY bpm, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        bpm,
        is_explicit_label,
        bpm_percent_rank,
        distinct_artist_country_in_release_date_month_is,
        bpm_change,
        bpm_avg_7,
        release_date_month_is_explicit_max_bpm,
        release_date_month_is_explicit_min_bpm,
        release_date_cume_dist,
        CONCAT(is_explicit_label, '#', CAST(bpm AS STRING)) AS is_explicit_label_ref
    FROM metrics
),
summary AS (
    -- Group-level totals
    SELECT
        is_explicit_label,
        COUNT(DISTINCT is_explicit_label) AS distinct_is_explicit_labels,
        ROUND(STDDEV(distinct_artist_country_in_release_date_month_is), 2) AS stddev_distinct_artist_country_in_release_date_m,
        ROUND(STDDEV(release_date_month_is_explicit_max_bpm), 2) AS stddev_release_date_month_is_explicit_max_bpm
    FROM scored
    GROUP BY is_explicit_label
    HAVING SUM(bpm_percent_rank) > 0.2
),
metrics_2 AS (
    -- Rank and compare rows within groups
    SELECT
        is_explicit_label,
        stddev_distinct_artist_country_in_release_date_m,
        stddev_release_date_month_is_explicit_max_bpm,
        NTH_VALUE(distinct_is_explicit_labels, 2) OVER (PARTITION BY is_explicit_label ORDER BY distinct_is_explicit_labels DESC NULLS FIRST, is_explicit_label ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_distinct_is_explicit_labels,
        NTILE(3) OVER (PARTITION BY distinct_is_explicit_labels ORDER BY stddev_release_date_month_is_explicit_max_bpm, is_explicit_label) AS stddev_release_date_month_is_explicit_max_bpm_te,
        ROUND(AVG(distinct_is_explicit_labels) OVER (PARTITION BY is_explicit_label ORDER BY distinct_is_explicit_labels DESC NULLS FIRST, is_explicit_label ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS distinct_is_explicit_labels_avg_1_prior
    FROM summary
)
SELECT is_explicit_label, stddev_release_date_month_is_explicit_max_bpm
FROM metrics_2
ORDER BY is_explicit_label, stddev_release_date_month_is_explicit_max_bpm
