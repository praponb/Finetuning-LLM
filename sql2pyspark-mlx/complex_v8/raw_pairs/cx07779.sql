WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        listener_id,
        track_id,
        played_at,
        ms_played,
        device,
        is_skipped,
        source,
        LENGTH(TRIM(device)) AS device_len,
        DATEDIFF(DATE '2026-01-01', played_at) AS days_since_played_at
    FROM plays
    WHERE source IN ('album', 'search', 'playlist')
),
without_matches AS (
    -- Rows with no matching plays
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM plays p
            WHERE p.track_id = s.track_id
              AND p.device IN ('desktop', 'phone', 'speaker')
              AND MONTH(p.played_at) IN (1, 3)
        )
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        listener_id,
        played_at,
        device,
        source,
        LENGTH(TRIM(device)) AS device_len_2,
        CONCAT(source, '#', CAST(listener_id AS STRING)) AS source_ref,
        device_len + ms_played AS device_len_plus_ms_played
    FROM without_matches
),
tagged AS (
    -- Add calculated columns
    SELECT *, INITCAP(TRIM(source)) AS source_clean
    FROM annotated
    WHERE listener_id IS NOT NULL
),
enriched_listener AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.listener_id,
        s.played_at,
        s.device,
        s.source,
        s.device_len_2,
        s.device_len_plus_ms_played,
        s.source_clean,
        l.plan AS listener_plan,
        l.signup_date AS listener_signup_date
    FROM tagged s
    JOIN listeners l ON l.id = s.listener_id
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        listener_id,
        device,
        source,
        device_len_2,
        device_len_plus_ms_played,
        listener_plan,
        listener_signup_date,
        ROUND(device_len_2 - AVG(device_len_2) OVER (PARTITION BY listener_plan, listener_id), 2) AS device_len_2_vs_listener_plan_listener_id_avg,
        MIN(device_len_2) OVER (PARTITION BY device_len_2 ORDER BY listener_signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_device_len_2_so_far
    FROM enriched_listener
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        listener_id,
        source,
        device_len_2,
        listener_plan,
        device_len_2_vs_listener_plan_listener_id_avg,
        ROUND(device_len_plus_ms_played / NULLIF(SUM(device_len_plus_ms_played) OVER (PARTITION BY listener_plan, source), 0), 4) AS device_len_plus_ms_played_share_of_listener_plan,
        LAG(device_len_2, 2, 0) OVER (ORDER BY min_device_len_2_so_far ASC, id) AS prev2_device_len_2,
        NTILE(3) OVER w_all_by_min_device_len_2_so_f AS min_device_len_2_so_far_tercile,
        ROUND(device_len_plus_ms_played - AVG(device_len_plus_ms_played) OVER (PARTITION BY listener_plan, source), 2) AS device_len_plus_ms_played_vs_listener_plan_sourc,
        min_device_len_2_so_far - LAG(min_device_len_2_so_far) OVER w_all_by_min_device_len_2_so_f AS min_device_len_2_so_far_change,
        ROUND(PERCENT_RANK() OVER (ORDER BY min_device_len_2_so_far ASC, id), 3) AS min_device_len_2_so_far_percent_rank
    FROM metrics
    WINDOW w_all_by_min_device_len_2_so_f AS (ORDER BY min_device_len_2_so_far ASC, id)
)
SELECT
    id,
    listener_id,
    source,
    device_len_2,
    listener_plan,
    device_len_2_vs_listener_plan_listener_id_avg,
    device_len_plus_ms_played_share_of_listener_plan,
    min_device_len_2_so_far_change,
    min_device_len_2_so_far_percent_rank
FROM with_windows
ORDER BY device_len_2_vs_listener_plan_listener_id_avg NULLS LAST, listener_id DESC, id
LIMIT 20
