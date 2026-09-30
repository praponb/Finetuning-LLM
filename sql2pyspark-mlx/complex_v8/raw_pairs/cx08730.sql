WITH prepared AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        ms_played,
        is_skipped,
        source
    FROM plays
    WHERE source IS NOT NULL
),
enriched AS (
    -- Join listeners
    SELECT
        s.id,
        s.listener_id,
        s.track_id,
        s.is_skipped,
        s.source,
        l.signup_date AS listener_signup_date,
        l.country AS listener_country,
        l.plan AS listener_plan
    FROM prepared s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        CASE WHEN listener_plan IN ('premium', 'free') THEN 'group_a' WHEN listener_plan = 'student' THEN 'group_b' ELSE 'other' END AS listener_plan_group,
        SUBSTRING(TRIM(listener_country), 1, 4) AS listener_country_prefix,
        CASE WHEN listener_country IN ('JP', 'UK') THEN 'group_a' WHEN listener_country = 'TH' THEN 'group_b' ELSE 'other' END AS listener_country_group
    FROM enriched
    WHERE track_id IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(listener_signup_date, 2) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_listener_signup_date,
        ROUND(CUME_DIST() OVER w_all_by_listener_signup_date_2, 3) AS listener_signup_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY listener_signup_date), 3) AS listener_signup_date_percent_rank,
        LAST_VALUE(listener_signup_date) OVER (ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_listener_signup_date,
        DATEDIFF(listener_signup_date, LAG(listener_signup_date) OVER w_all_by_listener_signup_date_3) AS days_since_prev_listener_signup_date
    FROM tagged
    WINDOW w_all_by_listener_signup_date_2 AS (ORDER BY listener_signup_date), w_all_by_listener_signup_date_3 AS (ORDER BY listener_signup_date, id)
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        LENGTH(TRIM(source)) AS source_len,
        DATE_FORMAT(listener_signup_date, 'yyyy-MM') AS listener_signup_date_label,
        DATE_FORMAT(listener_signup_date, 'yyyy-MM') AS listener_signup_date_label_2
    FROM with_trends
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, source_len, days_since_prev_listener_signup_date
    FROM scored
) src
UNPIVOT (
    amount FOR measure IN (source_len, days_since_prev_listener_signup_date)
) unp
