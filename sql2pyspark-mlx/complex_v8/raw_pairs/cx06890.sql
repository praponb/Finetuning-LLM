WITH cleaned_tracks AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods,
        ARRAY_JOIN(ARRAY_SORT(moods), ',') AS moods_text
    FROM tracks
    WHERE artist_id >= 3
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM cleaned_tracks
    WHERE duration_sec > (SELECT MIN(duration_sec) FROM cleaned_tracks)
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        FIRST_VALUE(duration_sec) OVER (PARTITION BY is_explicit, artist_id ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_duration_sec,
        ROW_NUMBER() OVER w_artist_id_by_duration_sec AS rn,
        ROUND(PERCENT_RANK() OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST), 3) AS duration_sec_percent_rank,
        RANK() OVER (PARTITION BY artist_id ORDER BY duration_sec DESC) AS duration_sec_rank,
        MAX(duration_sec) OVER (PARTITION BY is_explicit ORDER BY duration_sec NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_duration_sec_so_far
    FROM with_activity
    WINDOW w_artist_id_by_duration_sec AS (PARTITION BY artist_id ORDER BY duration_sec DESC, id)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        artist_id,
        title,
        duration_sec,
        release_date,
        is_explicit,
        bpm,
        moods_text,
        first_duration_sec,
        rn,
        duration_sec_percent_rank,
        duration_sec_rank,
        max_duration_sec_so_far,
        MONTH(release_date) AS release_date_month,
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_label
    FROM metrics
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        rn,
        SUM(CASE WHEN is_explicit = TRUE THEN 1 ELSE 0 END) AS n_is_explicit_true,
        MIN(first_duration_sec) AS min_first_duration_sec,
        PERCENTILE_APPROX(duration_sec, 0.5) AS median_duration_sec
    FROM tagged
    GROUP BY rn
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM grouped
    WHERE min_first_duration_sec > (SELECT AVG(min_first_duration_sec) FROM grouped)
),
annotated AS (
    -- Label and score rows
    SELECT
        rn,
        n_is_explicit_true,
        min_first_duration_sec,
        COALESCE(median_duration_sec, 0) AS median_duration_sec_filled,
        CASE WHEN median_duration_sec >= 338 THEN 'HIGH' WHEN median_duration_sec >= 172 THEN 'MEDIUM' ELSE 'LOW' END AS median_duration_sec_tier
    FROM screened
)
SELECT
    rn,
    n_is_explicit_true,
    PERCENTILE_APPROX(median_duration_sec_filled, 0.5) AS median_median_duration_sec_filled,
    COUNT(DISTINCT median_duration_sec_tier) AS distinct_median_duration_sec_tiers
FROM annotated
GROUP BY CUBE(rn, n_is_explicit_true)
ORDER BY rn NULLS LAST, n_is_explicit_true NULLS LAST
