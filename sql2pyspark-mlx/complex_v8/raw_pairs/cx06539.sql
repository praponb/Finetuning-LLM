WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source
    FROM plays
    WHERE (is_skipped = TRUE OR TO_DATE(played_at) < DATE '2025-02-18')
),
joined_listener AS (
    -- Attach listener attributes
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.played_at,
        s.ms_played,
        s.source,
        l.signup_date AS listener_signup_date,
        l.interests AS listener_interests,
        l.country AS listener_country
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
    WHERE YEAR(l.signup_date) = 2025
),
grouped AS (
    -- Summarise per group
    SELECT
        listener_country,
        DATE_FORMAT(listener_signup_date, 'yyyy-MM') AS listener_signup_date_ym,
        TO_DATE(played_at) AS played_at_day,
        COUNT(*) FILTER (WHERE source NOT IN ('radio', 'playlist')) AS n_source_not_in_radio_playlist,
        COUNT(*) FILTER (WHERE listener_country <> 'JP') AS n_listener_country_jp,
        MIN(played_at) AS first_played_at,
        COUNT(DISTINCT listener_country) AS distinct_listener_countrys,
        SUM(ms_played) FILTER (WHERE listener_country NOT IN ('TH', 'UK')) AS ms_played_listener_country_not_in_th_u
    FROM joined_listener
    WHERE YEAR(listener_signup_date) = 2025
    GROUP BY listener_country, DATE_FORMAT(listener_signup_date, 'yyyy-MM'), TO_DATE(played_at)
)
SELECT listener_country, listener_signup_date_ym, field, reading
FROM (
    SELECT listener_country, listener_signup_date_ym, n_listener_country_jp, distinct_listener_countrys, ms_played_listener_country_not_in_th_u
    FROM grouped
) src
UNPIVOT (
    reading FOR field IN (n_listener_country_jp AS n_listener_country_jp_v, distinct_listener_countrys AS distinct_listener_countrys_v, ms_played_listener_country_not_in_th_u AS m_ms_played_listener_country_not_in_th_u)
) unp
