WITH prepared AS (
    -- Filter tickets
    SELECT
        id,
        requester_id,
        agent_id,
        closed_at,
        tags,
        DATE_TRUNC('MONTH', opened_at) AS opened_at_month,
        CONCAT(priority, '#', CAST(agent_id AS STRING)) AS priority_ref,
        GREATEST(csat, requester_id) AS greatest_csat_requester_id
    FROM tickets
    WHERE csat < 4
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        closed_at,
        tags,
        priority_ref,
        SUBSTRING(TRIM(priority_ref), 1, 4) AS priority_ref_prefix,
        FLOOR(requester_id / 5) * 5 AS requester_id_bucket
    FROM prepared
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT *, RANK() OVER (ORDER BY closed_at DESC NULLS FIRST) AS closed_at_rank
    FROM enriched
    WINDOW w_all_by_closed_at_ties AS (ORDER BY closed_at DESC NULLS FIRST)
)
SELECT
    id,
    closed_at,
    tags,
    priority_ref_prefix,
    requester_id_bucket,
    closed_at_rank,
    CASE WHEN closed_at_rank >= 6 THEN 'large' WHEN closed_at_rank >= 5 THEN 'medium' ELSE 'small' END AS closed_at_rank_tier
FROM with_windows
