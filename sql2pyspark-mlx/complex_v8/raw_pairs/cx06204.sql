WITH filtered AS (
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
        INITCAP(TRIM(title)) AS title_clean,
        FLOOR(duration_sec / 50) * 50 AS duration_sec_bucket,
        DATE_ADD(release_date, 365) AS release_date_plus_365d
    FROM tracks
    WHERE artist_id >= 12
      AND bpm <= 155
),
artist_id_stats AS (
    -- Max bpm per artist_id
    SELECT artist_id, MAX(bpm) AS max_bpm
    FROM filtered
    GROUP BY artist_id
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT s.*, g.max_bpm, ROUND(s.bpm - g.max_bpm, 2) AS bpm_minus_max
    FROM filtered s
    JOIN artist_id_stats g ON g.artist_id = s.artist_id
    WHERE s.bpm = g.max_bpm
),
enriched AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.artist_id,
        s.duration_sec,
        s.release_date,
        s.moods,
        s.release_date_plus_365d,
        s.max_bpm,
        s.bpm_minus_max,
        a.debut_year AS artist_debut_year,
        a.genre AS artist_genre
    FROM vs_group s
    LEFT JOIN artists a ON a.id = s.artist_id
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(release_date, 'yyyy-MM') AS release_date_ym,
        artist_id,
        ROUND(STDDEV(max_bpm), 2) AS stddev_max_bpm,
        COUNT(DISTINCT artist_id) AS distinct_artist_ids
    FROM enriched
    GROUP BY DATE_FORMAT(release_date, 'yyyy-MM'), artist_id
    HAVING MAX(duration_sec) > 164
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        CASE WHEN distinct_artist_ids >= 7 THEN 'large' WHEN distinct_artist_ids >= 5 THEN 'medium' ELSE 'small' END AS distinct_artist_ids_tier,
        CONCAT(release_date_ym, '#', CAST(distinct_artist_ids AS STRING)) AS release_date_ym_ref,
        LENGTH(TRIM(release_date_ym)) AS release_date_ym_len
    FROM rollup_base
),
unique_rows AS (
    -- Keep the latest row per distinct_artist_ids_tier
    SELECT
        release_date_ym,
        artist_id,
        stddev_max_bpm,
        distinct_artist_ids,
        distinct_artist_ids_tier,
        release_date_ym_ref,
        release_date_ym_len
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY distinct_artist_ids_tier ORDER BY release_date_ym_len DESC, release_date_ym DESC, artist_id DESC) AS rn
        FROM scored
    ) ranked
    WHERE rn = 1
)
SELECT release_date_ym, distinct_artist_ids, distinct_artist_ids_tier
FROM unique_rows
WHERE distinct_artist_ids_tier = 'large'
