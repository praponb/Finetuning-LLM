WITH cleaned_artists AS (
    -- Filter artists
    SELECT
        id,
        artist_name,
        genre,
        country,
        debut_year,
        is_verified,
        CONCAT_WS('|', country, artist_name) AS country_artist_name_label,
        CASE WHEN country IN ('US', 'KR') THEN 'group_a' WHEN country = 'TH' THEN 'group_b' ELSE 'other' END AS country_group,
        CASE WHEN debut_year >= 2016 THEN 'large' WHEN debut_year >= 1997 THEN 'medium' ELSE 'small' END AS debut_year_tier
    FROM artists
    WHERE debut_year > 1997
      AND is_verified = TRUE
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        genre,
        debut_year,
        is_verified,
        debut_year_tier,
        LOWER(TRIM(country)) AS country_clean
    FROM cleaned_artists
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(debut_year, 0) AS debut_year_filled,
        CASE WHEN debut_year >= 2018 THEN 'HIGH' WHEN debut_year >= 1996 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier_2
    FROM scored
),
detail_rows AS (
    -- One row per track
    SELECT
        s.id,
        s.genre,
        s.debut_year,
        s.is_verified,
        s.country_clean,
        s.debut_year_filled,
        t.id AS track_id,
        t.release_date AS track_release_date,
        t.moods AS track_moods,
        t.is_explicit AS track_is_explicit,
        t.title AS track_title
    FROM labeled s
    JOIN tracks t ON t.artist_id = s.id
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        debut_year,
        is_verified,
        debut_year_filled,
        track_id,
        track_moods,
        track_title,
        COALESCE(debut_year, 0) AS debut_year_filled_2
    FROM detail_rows
    WHERE SIZE(track_moods) >= 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN is_verified THEN 1 ELSE 0 END AS is_verified_flag,
        CASE WHEN debut_year >= 2015 THEN 'HIGH' WHEN debut_year >= 1996 THEN 'MEDIUM' ELSE 'LOW' END AS debut_year_tier,
        CASE WHEN debut_year >= 2015 THEN 'A' WHEN debut_year >= 1997 THEN 'B' ELSE 'C' END AS debut_year_tier_2
    FROM enriched
)
SELECT
    id,
    is_verified,
    debut_year_filled,
    track_id,
    track_title,
    debut_year_filled_2,
    is_verified_flag,
    debut_year_tier,
    debut_year_tier_2,
    COALESCE(debut_year_tier, 'UNKNOWN') AS debut_year_tier_or_unknown
FROM annotated
ORDER BY debut_year_tier_2 DESC, debut_year_filled_2 DESC NULLS FIRST
