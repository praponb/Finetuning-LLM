WITH all_events AS (
    -- Stack the branches
    SELECT product, id AS ref_id
    FROM policies
    UNION
    SELECT status AS product, id AS ref_id
    FROM claims
    WHERE claim_date BETWEEN DATE '2024-04-14' AND DATE '2025-06-24'
)
SELECT product, COUNT(*) AS n_rows, SUM(ref_id) AS total_ref_id
FROM all_events
GROUP BY product
ORDER BY product
