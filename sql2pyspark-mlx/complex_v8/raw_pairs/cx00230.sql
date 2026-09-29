WITH stacked AS (
    -- Stack the branches
    SELECT
        product AS name,
        start_date AS day,
        holder_id,
        'policy' AS source
    FROM policies
    WHERE product NOT IN ('LIFE', 'HOME')
    UNION ALL
    SELECT
        product AS name,
        start_date AS day,
        holder_id,
        'policy_2' AS source
    FROM policies
    WHERE start_date BETWEEN DATE '2024-08-27' AND DATE '2025-10-13'
    UNION ALL
    SELECT
        claim_type AS name,
        claim_date AS day,
        id AS holder_id,
        'claim' AS source
    FROM claims
)
SELECT source, COUNT(*) AS n_rows, SUM(holder_id) AS total_holder_id, MAX(day) AS latest_day
FROM stacked
GROUP BY source
ORDER BY source
