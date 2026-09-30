WITH base AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        YEAR(reported_at) AS reported_at_year
    FROM claims
),
combined AS (
    -- Stack the branches
    SELECT 'claim' AS source, id, adjuster_id AS entity_id
    FROM base
    UNION ALL
    SELECT 'claim_2' AS source, policy_id AS id, adjuster_id AS entity_id
    FROM claims
)
SELECT source, COUNT(*) AS n_rows, SUM(id) AS total_id
FROM combined
GROUP BY source
ORDER BY source
