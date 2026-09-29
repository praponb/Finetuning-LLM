WITH recent AS (
    -- Scope the tracks rows
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label,
        DATEDIFF(DATE '2026-01-01', release_date) AS days_since_release_date,
        SIZE(moods) AS moods_count
    FROM tracks
    WHERE (duration_sec <= 357 OR bpm < 170)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        bpm,
        moods,
        release_date_label,
        days_since_release_date,
        ROUND(days_since_release_date / NULLIF(SUM(days_since_release_date) OVER (PARTITION BY is_explicit, moods_count), 0), 4) AS days_since_release_date_share_of_is_explicit_moo,
        ROUND(duration_sec / NULLIF(SUM(duration_sec) OVER (PARTITION BY is_explicit, moods_count), 0), 4) AS duration_sec_share_of_is_explicit_moods_count
    FROM recent
),
rollup_base AS (
    -- Group-level totals
    SELECT
        artist_id,
        ROUND(SUM(days_since_release_date) / NULLIF(SUM(duration_sec_share_of_is_explicit_moods_count), 0), 3) AS days_since_release_date_to_duration_sec_share_of,
        SUM(CASE WHEN LENGTH(TRIM(title)) >= 6 THEN 1 ELSE 0 END) AS n_length_trim_title_6,
        MAX(days_since_release_date_share_of_is_explicit_moo) AS max_days_since_release_date_share_of_is_explicit,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids,
        MAX(duration_sec) AS max_duration_sec
    FROM metrics
    GROUP BY artist_id
),
labeled AS (
    -- Add calculated columns
    SELECT
        artist_id,
        days_since_release_date_to_duration_sec_share_of,
        distinct_artist_ids,
        CASE WHEN distinct_artist_ids >= 8 THEN 'large' WHEN distinct_artist_ids >= 3 THEN 'medium' ELSE 'small' END AS distinct_artist_ids_tier,
        ABS(n_length_trim_title_6 - 6) AS n_length_trim_title_6_distance,
        ROUND(max_duration_sec / NULLIF(n_length_trim_title_6, 0), 3) AS max_duration_sec_per_n_length_trim_title_6
    FROM rollup_base
),
labeled_2 AS (
    -- Label and score rows
    SELECT
        artist_id,
        days_since_release_date_to_duration_sec_share_of,
        distinct_artist_ids,
        distinct_artist_ids_tier,
        max_duration_sec_per_n_length_trim_title_6,
        LENGTH(TRIM(distinct_artist_ids_tier)) AS distinct_artist_ids_tier_len
    FROM labeled
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM labeled_2
    WHERE distinct_artist_ids_tier_len < (SELECT MAX(distinct_artist_ids_tier_len) FROM labeled_2)
)
SELECT *
FROM (
    SELECT distinct_artist_ids_tier_len, artist_id, distinct_artist_ids_tier, max_duration_sec_per_n_length_trim_title_6
    FROM with_activity
) src
PIVOT (
    SUM(max_duration_sec_per_n_length_trim_title_6) AS amount, COUNT(max_duration_sec_per_n_length_trim_title_6) AS n
    FOR distinct_artist_ids_tier IN ('large', 'medium')
)
