WITH filtered_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        opened_at,
        closed_at,
        channel,
        csat,
        DATE_FORMAT(opened_at, 'yyyy-MM-dd') AS opened_at_label,
        CONCAT(priority, '#', CAST(id AS STRING)) AS priority_ref,
        INITCAP(TRIM(priority)) AS priority_clean
    FROM tickets
    WHERE requester_id BETWEEN 7 AND 24
      AND priority = 'P4'
),
per_group AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(closed_at, 'yyyy-MM') AS closed_at_ym,
        DATE_FORMAT(opened_at, 'yyyy-MM') AS opened_at_ym,
        csat,
        SUM(CASE WHEN channel IN ('chat', 'email', 'phone') THEN 1 ELSE 0 END) AS n_channel_in_chat_email_phone,
        COUNT(DISTINCT channel) AS distinct_channels,
        ARRAY_SORT(COLLECT_SET(channel)) AS channel_list
    FROM filtered_tickets
    GROUP BY DATE_FORMAT(closed_at, 'yyyy-MM'), DATE_FORMAT(opened_at, 'yyyy-MM'), csat
    HAVING MAX(csat) > 1
),
tagged AS (
    -- Derive helper columns
    SELECT
        closed_at_ym,
        opened_at_ym,
        csat,
        distinct_channels,
        channel_list,
        UPPER(TRIM(closed_at_ym)) AS closed_at_ym_clean,
        CASE WHEN csat >= 4 THEN 'large' WHEN csat >= 2 THEN 'medium' ELSE 'small' END AS csat_tier
    FROM per_group
    WHERE n_channel_in_chat_email_phone BETWEEN 2 AND 9
)
SELECT *
FROM (
    SELECT csat, csat_tier, distinct_channels
    FROM tagged
) src
PIVOT (
    SUM(distinct_channels) AS amount, MAX(distinct_channels) AS max_v
    FOR csat_tier IN ('small', 'large', 'medium')
)
