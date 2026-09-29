WITH unioned AS (
    -- Stack the branches
    SELECT
        'policy' AS source,
        holder_id AS qty,
        end_date,
        product AS category
    FROM policies
    UNION ALL
    SELECT
        'claim' AS source,
        policy_id AS qty,
        claim_date AS end_date,
        status AS category
    FROM claims
    WHERE adjuster_id <= 3
      AND claimed_amount <= 32878.5
)
SELECT source, COUNT(*) AS n_rows, SUM(qty) AS total_qty, MAX(end_date) AS latest_end_date
FROM unioned
GROUP BY source
ORDER BY source
