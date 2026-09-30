WITH valid AS (
    -- Scope the listeners rows
    SELECT
        id,
        username,
        country,
        signup_date,
        birth_year,
        interests,
        LOWER(REGEXP_REPLACE(TRIM(plan), ' ', '_')) AS plan_slug,
        COALESCE(country, 'UNKNOWN') AS country_or_unknown
    FROM listeners
    WHERE birth_year >= 1966
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_ADD(signup_date, 30) AS signup_date_plus_30d,
        ABS(birth_year - 1970) AS birth_year_distance
    FROM valid
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        ARRAY_JOIN(ARRAY_SORT(interests), ',') AS interests_text,
        DATEDIFF(DATE '2025-06-30', signup_date_plus_30d) AS days_since_signup_date_plus_30d,
        CONCAT(plan_slug, '#', CAST(birth_year AS STRING)) AS plan_slug_ref
    FROM enriched
    WHERE country_or_unknown IN ('KR', 'BR')
),
exploded AS (
    -- One row per interest
    SELECT
        s.id,
        s.username,
        s.country,
        s.signup_date,
        s.birth_year,
        s.plan_slug,
        s.country_or_unknown,
        s.signup_date_plus_30d,
        s.birth_year_distance,
        s.interests_text,
        s.days_since_signup_date_plus_30d,
        s.plan_slug_ref,
        t.interest
    FROM labeled s
    LATERAL VIEW EXPLODE(s.interests) t AS interest
),
with_children AS (
    -- One row per play
    SELECT
        s.id,
        s.country,
        s.country_or_unknown,
        s.interests_text,
        s.days_since_signup_date_plus_30d,
        s.plan_slug_ref,
        s.interest,
        p.id AS play_id,
        p.played_at AS play_played_at,
        p.source AS play_source,
        p.device AS play_device
    FROM exploded s
    LEFT JOIN plays p ON p.listener_id = s.id
),
exploded_detail AS (
    -- One row per play
    SELECT
        s.id,
        s.country,
        s.country_or_unknown,
        s.interests_text,
        s.plan_slug_ref,
        s.interest,
        s.play_id,
        s.play_played_at,
        s.play_source,
        s.play_device,
        p.id AS play_id_2,
        p.is_skipped AS play_is_skipped,
        p.played_at AS play_played_at_2,
        p.track_id AS play_track_id
    FROM with_children s
    JOIN plays p ON p.listener_id = s.id
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(play_played_at) IGNORE NULLS OVER (PARTITION BY play_id, play_track_id ORDER BY play_played_at_2 ASC, id, interest, play_id, play_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_play_played_at,
        FIRST_VALUE(play_played_at_2) OVER (PARTITION BY play_id, play_track_id ORDER BY play_played_at_2 ASC, id, interest, play_id, play_id_2 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_play_played_at_2,
        ROUND(CUME_DIST() OVER w_play_id_play_track_id_by_pla_3, 3) AS play_played_at_2_cume_dist,
        ROUND(PERCENT_RANK() OVER w_play_id_play_track_id_by_pla_4, 3) AS play_played_at_2_percent_rank
    FROM exploded_detail
    WINDOW w_play_id_play_track_id_by_pla_3 AS (PARTITION BY play_id, play_track_id ORDER BY play_played_at_2 ASC, id, interest, play_id, play_id_2), w_play_id_play_track_id_by_pla_4 AS (PARTITION BY play_id, play_track_id ORDER BY play_played_at_2 ASC)
)
SELECT id, interest, measure, metric_value
FROM (
    SELECT id, interest, play_played_at_2_cume_dist, play_played_at_2_percent_rank
    FROM ranked
) src
UNPIVOT (
    metric_value FOR measure IN (play_played_at_2_cume_dist, play_played_at_2_percent_rank)
) unp
ORDER BY id, interest, measure
