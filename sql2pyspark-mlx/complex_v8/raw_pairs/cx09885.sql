WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        CASE WHEN segment IN ('VIP', 'SME') THEN 'group_a' WHEN segment = 'RETAIL' THEN 'group_b' ELSE 'other' END AS segment_group
    FROM policyholders
)
SELECT segment AS label, id, 'holder' AS src
FROM base
WHERE segment NOT IN ('SME', 'RETAIL')
  AND segment_group <> 'group_b'
UNION ALL
SELECT adjuster_name AS label, level AS id, 'adjuster' AS src
FROM adjusters
WHERE level < 3
UNION ALL
SELECT segment AS label, id, 'holder_3' AS src
FROM policyholders
WHERE UPPER(TRIM(holder_name)) LIKE 'B%'
