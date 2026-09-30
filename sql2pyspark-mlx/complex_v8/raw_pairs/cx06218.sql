WITH recent AS (
    -- Scope the plays rows
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        TO_DATE(played_at) AS played_at_date,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE played_at < TIMESTAMP '2025-02-18 10:00:00'
),
with_parent_listener AS (
    -- Attach listener attributes
    SELECT
        s.*,
        l.country AS listener_country,
        l.username AS listener_username,
        l.plan AS listener_plan
    FROM recent s
    LEFT JOIN listeners l ON l.id = s.listener_id
),
latest_per_key AS (
    -- Keep the earliest row per device, listener_id
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        played_at_date,
        days_since_played_at,
        listener_country,
        listener_username,
        listener_plan
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY device, listener_id ORDER BY played_at, id) AS row_num
        FROM with_parent_listener
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTILE(4) OVER (PARTITION BY track_id ORDER BY days_since_played_at, id) AS days_since_played_at_quartile,
        LEAD(played_at_date, 3) OVER (PARTITION BY track_id ORDER BY days_since_played_at, id) AS next3_played_at_date,
        ROW_NUMBER() OVER w_listener_id_by_ms_played AS ms_played_seq,
        ROUND(AVG(days_since_played_at) OVER (PARTITION BY track_id ORDER BY days_since_played_at, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS days_since_played_at_avg_1_prior,
        COUNT(*) OVER w_listener_plan AS rows_in_listener_plan
    FROM latest_per_key
    WINDOW w_listener_id_by_ms_played AS (PARTITION BY listener_id ORDER BY ms_played, id), w_listener_plan AS (PARTITION BY listener_plan)
)
-- Final projection
SELECT
    id,
    listener_id,
    track_id,
    played_at,
    device,
    played_at_date,
    days_since_played_at,
    listener_country,
    listener_username,
    listener_plan,
    days_since_played_at_quartile,
    next3_played_at_date,
    ms_played_seq,
    rows_in_listener_plan,
    CONCAT_WS(' / ', listener_plan, listener_username) AS listener_plan_listener_username_label
FROM metrics
WHERE ms_played >= 168782
ORDER BY next3_played_at_date NULLS LAST
