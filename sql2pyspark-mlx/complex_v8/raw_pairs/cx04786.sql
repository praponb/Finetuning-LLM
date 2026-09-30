WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product IN ('LIFE', 'TRAVEL')
      AND holder_id <= 5
),
stacked AS (
    -- Stack the branches
    SELECT
        holder_id AS entity_id,
        id,
        premium AS value,
        'policy' AS src
    FROM cleaned_policys
    WHERE premium > 1051.9
      AND start_date BETWEEN DATE '2024-03-15' AND DATE '2025-05-30'
    UNION ALL
    SELECT
        id,
        policy_id,
        approved_amount,
        'claim' AS src
    FROM claims
    WHERE adjuster_id <= 3
)
SELECT src, COUNT(*) AS n_rows, SUM(entity_id) AS total_entity_id
FROM stacked
GROUP BY src
ORDER BY src
