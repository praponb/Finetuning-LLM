WITH scoped AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags
    FROM tickets
    WHERE csat >= 2
),
rollup_base AS (
    -- Summarise per group
    SELECT
        channel,
        csat,
        MAX(closed_at) AS last_closed_at,
        COUNT(*) AS n_rows,
        PERCENTILE_APPROX(csat, 0.5) AS median_csat,
        ROUND(AVG(requester_id), 2) AS avg_requester_id,
        MIN(requester_id) AS min_requester_id
    FROM scoped
    GROUP BY 1, 2
),
derived AS (
    -- Derive helper columns
    SELECT
        channel,
        csat,
        last_closed_at,
        median_csat,
        avg_requester_id,
        min_requester_id,
        CASE WHEN csat >= 4 THEN 'HIGH' WHEN csat >= 3 THEN 'MEDIUM' ELSE 'LOW' END AS csat_tier,
        DATE_FORMAT(last_closed_at, 'yyyy-MM') AS last_closed_at_label
    FROM rollup_base
    WHERE n_rows BETWEEN 4 AND 11
),
derived_2 AS (
    -- Derive helper columns
    SELECT
        channel,
        csat,
        median_csat,
        min_requester_id,
        last_closed_at_label,
        LENGTH(TRIM(last_closed_at_label)) AS last_closed_at_label_len,
        DATEDIFF(DATE '2025-06-30', last_closed_at) AS days_since_last_closed_at
    FROM derived
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived_2
    WHERE median_csat > (SELECT AVG(median_csat) FROM derived_2)
),
derived_3 AS (
    -- Derive helper columns
    SELECT
        *,
        LOWER(REGEXP_REPLACE(TRIM(channel), ' ', '_')) AS channel_slug,
        LEAST(min_requester_id, last_closed_at_label_len) AS least_min_requester_id_last_closed_at_label_len,
        last_closed_at_label_len + median_csat AS last_closed_at_label_len_plus_median_csat
    FROM screened
)
SELECT *
FROM (
    SELECT median_csat, channel, least_min_requester_id_last_closed_at_label_len
    FROM derived_3
) src
PIVOT (
    SUM(least_min_requester_id_last_closed_at_label_len)
    FOR channel IN ('chat', 'phone', 'email', 'web')
)
ORDER BY median_csat
