WITH scoped AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit
    FROM tracks
    WHERE bpm < 143
      AND ARRAY_CONTAINS(moods, 'chill')
),
with_parent_artist AS (
    -- Attach artist attributes
    SELECT
        s.id,
        s.artist_id,
        s.title,
        s.release_date,
        s.is_explicit,
        a.country AS artist_country
    FROM scoped s
    LEFT JOIN artists a ON a.id = s.artist_id
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        artist_id,
        title,
        is_explicit,
        artist_country,
        ROW_NUMBER() OVER (PARTITION BY YEAR(release_date) ORDER BY release_date NULLS LAST, id) AS rn,
        LEAD(release_date) OVER (ORDER BY release_date DESC, id) AS next_release_date,
        RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_country ORDER BY release_date DESC) AS release_date_rank,
        LAG(release_date, 2) OVER w_all_by_release_date AS prev2_release_date,
        NTH_VALUE(release_date, 2) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_release_date,
        ROUND(CUME_DIST() OVER w_release_date_month_artist_co, 3) AS release_date_cume_dist,
        LEAD(release_date) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_country ORDER BY release_date DESC, id) AS next_release_date_2,
        LEAD(release_date, 2) OVER (ORDER BY release_date ASC, id) AS next2_release_date,
        FIRST_VALUE(release_date) OVER (ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date,
        FIRST_VALUE(release_date) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_country ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date_2,
        DATEDIFF(release_date, LAG(release_date) OVER (ORDER BY release_date ASC, id)) AS days_since_prev_release_date,
        FIRST_VALUE(release_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_country ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_release_date,
        FIRST_VALUE(release_date) OVER (ORDER BY release_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_release_date_3,
        LEAD(release_date, 3) OVER (ORDER BY release_date ASC, id) AS next3_release_date,
        NTILE(4) OVER w_all_by_release_date_2 AS release_date_quartile,
        ROW_NUMBER() OVER (ORDER BY release_date ASC, id) AS rn_2
    FROM with_parent_artist
    WINDOW w_all_by_release_date AS (ORDER BY release_date DESC, id), w_release_date_month_artist_co AS (PARTITION BY DATE_TRUNC('MONTH', release_date), artist_country ORDER BY release_date DESC), w_all_by_release_date_2 AS (ORDER BY release_date, id)
),
stats AS (
    -- Summarise per group
    SELECT
        DATE_TRUNC('MONTH', second_release_date) AS second_release_date_month,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE is_explicit = FALSE) AS n_is_explicit_false,
        SUM(CASE WHEN is_explicit = FALSE THEN release_date_cume_dist ELSE 0 END) AS release_date_cume_dist_is_explicit_false,
        SUM(release_date_quartile) AS total_release_date_quartile
    FROM sequenced
    WHERE YEAR(first_release_date_2) = 2024
    GROUP BY 1
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        second_release_date_month,
        n_is_explicit_false,
        release_date_cume_dist_is_explicit_false,
        total_release_date_quartile,
        ROUND(CUME_DIST() OVER w_second_release_date_month_to, 3) AS n_is_explicit_false_cume_dist,
        ROW_NUMBER() OVER w_second_release_date_month_to_2 AS rn,
        SUM(distinct_ids) OVER (PARTITION BY distinct_ids, n_is_explicit_false) AS distinct_ids_n_is_explicit_false_sum_distinct_id,
        ROUND(n_is_explicit_false / NULLIF(SUM(n_is_explicit_false) OVER (PARTITION BY distinct_ids, n_is_explicit_false), 0), 4) AS n_is_explicit_false_share_of_distinct_ids_n_is_e
    FROM stats
    WINDOW w_second_release_date_month_to AS (PARTITION BY TO_DATE(second_release_date_month) ORDER BY n_is_explicit_false DESC), w_second_release_date_month_to_2 AS (PARTITION BY TO_DATE(second_release_date_month) ORDER BY n_is_explicit_false DESC, second_release_date_month)
)
-- Final projection
SELECT
    second_release_date_month,
    n_is_explicit_false,
    release_date_cume_dist_is_explicit_false,
    total_release_date_quartile,
    n_is_explicit_false_cume_dist,
    rn,
    n_is_explicit_false_share_of_distinct_ids_n_is_e,
    ROUND(rn / NULLIF(n_is_explicit_false, 0), 3) AS rn_per_n_is_explicit_false,
    FLOOR(rn / 2) * 2 AS rn_bucket
FROM windowed
ORDER BY rn_bucket DESC NULLS FIRST
