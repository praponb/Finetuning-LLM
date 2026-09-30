WITH prepared_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        risk_score,
        segment,
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_label
    FROM policyholders
    WHERE joined_on >= DATE '2024-06-18'
)
SELECT id
FROM prepared_holders
INTERSECT
SELECT level
FROM adjusters
ORDER BY id
