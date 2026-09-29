WITH scoped AS (
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
        ABS(requester_id - 18) AS requester_id_distance
    FROM tickets
    WHERE ARRAY_CONTAINS(tags, 'login')
      AND requester_id >= 5
),
enriched AS (
    -- Add calculated columns
    SELECT
        id,
        agent_id,
        closed_at,
        priority,
        csat,
        tags,
        priority_clean,
        LOWER(REGEXP_REPLACE(TRIM(priority), ' ', '_')) AS priority_slug,
        DATE_TRUNC('YEAR', closed_at) AS closed_at_year
    FROM scoped
    WHERE csat BETWEEN 1 AND 4
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        priority,
        csat,
        tags,
        priority_clean,
        closed_at_year,
        RANK() OVER (PARTITION BY MONTH(closed_at_year), agent_id ORDER BY closed_at DESC) AS closed_at_rank,
        DENSE_RANK() OVER (PARTITION BY TO_DATE(closed_at_year), DATE_TRUNC('MONTH', closed_at) ORDER BY csat) AS csat_dense_rank
    FROM enriched
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, measure, amount
FROM (
    SELECT id, csat_dense_rank, closed_at_rank, csat
    FROM sequenced
) src
UNPIVOT (
    amount FOR measure IN (csat_dense_rank, closed_at_rank, csat)
) unp
