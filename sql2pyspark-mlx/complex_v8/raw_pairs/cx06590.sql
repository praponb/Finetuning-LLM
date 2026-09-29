WITH valid_tracks AS (
    -- Filter tracks
    SELECT
        id,
        artist_id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        CONCAT(title, '#', CAST(artist_id AS STRING)) AS title_ref
    FROM tracks
    WHERE UPPER(TRIM(title)) LIKE 'C%'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        duration_sec,
        release_date,
        is_explicit,
        moods,
        title_ref,
        RANK() OVER w_artist_id_is_explicit_by_dur AS duration_sec_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY artist_id ORDER BY release_date, id), 3) AS release_date_cume_dist,
        LAG(release_date, 2) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id) AS prev2_release_date,
        SUM(duration_sec) OVER w_is_explicit AS is_explicit_sum_duration_sec,
        LEAD(release_date) OVER (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id) AS next_release_date,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit), 0), 4) AS duration_sec_share_of_release_date_month_is_expl,
        ROW_NUMBER() OVER w_artist_id_by_release_date AS rn,
        duration_sec - LAG(duration_sec) OVER (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id) AS duration_sec_change,
        COUNT(*) OVER (PARTITION BY is_explicit) AS rows_in_is_explicit,
        MAX(duration_sec) OVER w_release_date_month_is_explic AS release_date_month_is_explicit_max_duration_sec,
        LAST_VALUE(duration_sec) IGNORE NULLS OVER (PARTITION BY is_explicit ORDER BY release_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_duration_sec,
        SUM(duration_sec) OVER w_release_date_month_is_explic AS release_date_month_is_explicit_sum_duration_sec
    FROM valid_tracks
    WINDOW w_artist_id_is_explicit_by_dur AS (PARTITION BY artist_id, is_explicit ORDER BY duration_sec DESC, id), w_is_explicit AS (PARTITION BY is_explicit), w_artist_id_by_release_date AS (PARTITION BY artist_id ORDER BY release_date, id), w_release_date_month_is_explic AS (PARTITION BY DATE_TRUNC('MONTH', release_date), is_explicit), w_is_explicit_artist_id_by_rel AS (PARTITION BY is_explicit, artist_id ORDER BY release_date DESC, id)
),
top_per_group AS (
    -- First row per group
    SELECT *
    FROM metrics
    WHERE rn = 1
),
grouped AS (
    -- Group-level totals
    SELECT
        rn,
        DATE_TRUNC('MONTH', release_date) AS release_date_month,
        COUNT(*) AS cnt,
        COUNT(DISTINCT id) AS distinct_ids,
        MIN(prev2_release_date) AS first_prev2_release_date,
        ROUND(AVG(last_known_duration_sec), 2) AS avg_last_known_duration_sec,
        ROUND(SUM(rows_in_is_explicit) / NULLIF(SUM(duration_sec_share_of_release_date_month_is_expl), 0), 3) AS rows_in_is_explicit_to_duration_sec_share_of_rel
    FROM top_per_group
    GROUP BY rn, DATE_TRUNC('MONTH', release_date)
),
sequenced AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(rn) OVER (PARTITION BY distinct_ids ORDER BY distinct_ids, rn, release_date_month ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS rn_avg_4,
        COUNT(*) OVER (PARTITION BY rn, distinct_ids) AS rows_in_rn_distinct_ids,
        ROUND(rn - AVG(rn) OVER (PARTITION BY rn, distinct_ids), 2) AS rn_vs_rn_distinct_ids_avg
    FROM grouped
)
SELECT
    rn,
    distinct_ids,
    first_prev2_release_date,
    avg_last_known_duration_sec,
    rows_in_is_explicit_to_duration_sec_share_of_rel,
    rn_avg_4,
    rn_vs_rn_distinct_ids_avg,
    DATE_FORMAT(first_prev2_release_date, 'yyyy-MM') AS first_prev2_release_date_label
FROM sequenced
ORDER BY rn_vs_rn_distinct_ids_avg DESC
