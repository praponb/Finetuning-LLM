WITH filtered AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        CASE WHEN ms_played >= 220498 THEN 'A' WHEN ms_played >= 98052 THEN 'B' ELSE 'C' END AS ms_played_tier,
        COALESCE(source, 'UNKNOWN') AS source_or_unknown
    FROM plays
    WHERE device NOT IN ('tv', 'phone')
),
with_parent AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.ms_played,
        s.is_skipped,
        s.source,
        s.ms_played_tier,
        s.source_or_unknown,
        l.country AS listener_country,
        l.signup_date AS listener_signup_date
    FROM filtered s
    JOIN listeners l ON l.id = s.listener_id
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(ms_played) OVER (PARTITION BY source, listener_id) AS source_listener_id_min_ms_played,
        LAG(ms_played, 3) OVER (PARTITION BY listener_country, is_skipped ORDER BY listener_signup_date ASC, id) AS prev3_ms_played,
        ROUND(PERCENT_RANK() OVER (PARTITION BY listener_country, is_skipped ORDER BY ms_played DESC), 3) AS ms_played_percent_rank,
        SIZE(COLLECT_SET(source_or_unknown) OVER (PARTITION BY source, listener_id)) AS distinct_source_or_unknown_in_source_listener_id,
        ROUND(AVG(ms_played) OVER (PARTITION BY is_skipped, ms_played_tier ORDER BY ms_played, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS ms_played_avg_6_prior,
        FIRST_VALUE(listener_signup_date) OVER (PARTITION BY listener_country, is_skipped ORDER BY ms_played DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_listener_signup_date,
        ROUND(AVG(ms_played) OVER (PARTITION BY listener_country, is_skipped ORDER BY listener_signup_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS ms_played_avg_7,
        ms_played - LAG(ms_played) OVER (PARTITION BY listener_country, source_or_unknown ORDER BY ms_played NULLS LAST, id) AS ms_played_change,
        ROUND(ms_played - AVG(ms_played) OVER w_source_listener_id, 2) AS ms_played_vs_source_listener_id_avg,
        ROUND(ms_played / NULLIF(SUM(ms_played) OVER w_source_listener_id, 0), 4) AS ms_played_share_of_source_listener_id,
        ROW_NUMBER() OVER (PARTITION BY is_skipped, ms_played_tier ORDER BY ms_played, id) AS rn
    FROM with_parent
    WINDOW w_source_listener_id AS (PARTITION BY source, listener_id)
),
leaders AS (
    -- First row per group
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source,
        ms_played_tier,
        source_or_unknown,
        listener_country,
        listener_signup_date,
        source_listener_id_min_ms_played,
        prev3_ms_played,
        ms_played_percent_rank,
        distinct_source_or_unknown_in_source_listener_id,
        ms_played_avg_6_prior,
        first_listener_signup_date,
        ms_played_avg_7,
        ms_played_change,
        ms_played_vs_source_listener_id_avg,
        ms_played_share_of_source_listener_id
    FROM with_trends
    WHERE rn = 1
)
SELECT
    id,
    listener_id,
    source,
    ms_played_tier,
    listener_country,
    listener_signup_date,
    source_listener_id_min_ms_played,
    prev3_ms_played,
    ms_played_percent_rank,
    ms_played_avg_6_prior,
    first_listener_signup_date,
    ms_played_avg_7,
    ms_played_change,
    ms_played_vs_source_listener_id_avg,
    ms_played_share_of_source_listener_id,
    DATE_ADD(listener_signup_date, 30) AS listener_signup_date_plus_30d
FROM leaders
ORDER BY id
LIMIT 5
