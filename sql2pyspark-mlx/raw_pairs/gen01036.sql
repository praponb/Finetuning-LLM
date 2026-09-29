WITH base AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        ROUND(duration_sec * 100, 2) duration_sec_adjusted,
        COALESCE(page, 'DIRECT') resolved_page
    FROM web_events
    WHERE TO_DATE(event_time) < '2023-07-01'
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page, session_id ORDER BY duration_sec_adjusted DESC, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY rank_in_group DESC, id) AS row_num
        FROM latest
    ) x
    WHERE row_num = 1
),
enriched AS (
    SELECT
        a.*,
        s.device,
        s.country,
        s.browser
    FROM ranked a
    INNER JOIN sessions s
        ON a.session_id = s.id
)
SELECT
    id,
    session_id,
    event_type,
    page,
    duration_sec_adjusted,
    rank_in_group,
    row_num,
    device,
    country
FROM enriched
