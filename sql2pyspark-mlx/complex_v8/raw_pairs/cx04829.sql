WITH scoped_tickets AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        INITCAP(TRIM(priority)) AS priority_clean,
        ARRAY_JOIN(ARRAY_SORT(tags), ',') AS tags_text
    FROM tickets
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SIZE(COLLECT_SET(agent_id) OVER (PARTITION BY csat)) AS distinct_agent_id_in_csat,
        LEAD(csat, 1, 0) OVER (PARTITION BY channel, csat ORDER BY closed_at DESC, id) AS next_csat,
        MIN(requester_id) OVER (PARTITION BY csat) AS csat_min_requester_id,
        MAX(csat) OVER (ORDER BY csat, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS csat_max_3_prior
    FROM scoped_tickets
),
deduped AS (
    -- Keep the earliest row per priority
    SELECT
        id,
        requester_id,
        agent_id,
        opened_at,
        closed_at,
        priority,
        channel,
        csat,
        tags,
        priority_clean,
        tags_text,
        distinct_agent_id_in_csat,
        next_csat,
        csat_min_requester_id,
        csat_max_3_prior
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY priority ORDER BY closed_at, id) AS rn
        FROM metrics
    ) ranked
    WHERE rn = 1
),
qualified AS (
    -- Compare against the overall max
    SELECT *
    FROM deduped
    WHERE csat < (SELECT MAX(csat) FROM deduped)
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, metric_value
FROM (
    SELECT id, csat_max_3_prior, csat, distinct_agent_id_in_csat
    FROM qualified
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR field IN (csat_max_3_prior AS csat_max_3_prior_v, csat AS csat_v, distinct_agent_id_in_csat AS distinct_agent_id_in_csat_v)
) unp
