WITH cleaned_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        agent_id,
        priority,
        channel,
        csat,
        tags,
        SUBSTRING(TRIM(channel), 1, 3) AS channel_prefix
    FROM tickets
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        CASE WHEN csat >= 4 THEN 'HIGH' WHEN csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier
    FROM cleaned_tickets
    WHERE SIZE(tags) >= 1
),
totals AS (
    -- Summarise per group
    SELECT csat, COUNT(*) AS n_rows, SUM(csat) AS total_csat
    FROM scored
    GROUP BY 1
)
SELECT
    csat,
    n_rows,
    total_csat,
    CASE WHEN csat >= 4 THEN 'HIGH' WHEN csat >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier,
    COALESCE(csat, 0) AS csat_filled
FROM totals
ORDER BY csat_tier DESC, total_csat
