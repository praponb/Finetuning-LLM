WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        COALESCE(risk_score, 0.0) AS risk_score_filled
    FROM policyholders
    WHERE segment = 'RETAIL'
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM base s
    WHERE s.id IN (
            SELECT p.holder_id
            FROM policies p
            WHERE p.premium > 1541.8
              AND p.start_date BETWEEN DATE '2024-05-07' AND DATE '2025-05-28'
        )
      AND s.joined_on IS NOT NULL
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        holder_name,
        region,
        joined_on,
        risk_score_filled,
        CONCAT(holder_name, '#', CAST(id AS STRING)) AS holder_name_ref,
        CASE WHEN region IN ('South', 'North') THEN 'group_a' WHEN region = 'West' THEN 'group_b' ELSE 'other' END AS region_group,
        LENGTH(TRIM(holder_name)) AS holder_name_len
    FROM screened
    WHERE LENGTH(TRIM(holder_name)) >= 6
),
screened_2 AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM annotated s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.premium <= 3532.7
              AND p.end_date < DATE '2026-09-23'
        )
)
-- Final projection
SELECT
    id,
    holder_name,
    region,
    joined_on,
    holder_name_ref,
    region_group,
    holder_name_len
FROM screened_2
ORDER BY holder_name_len DESC, holder_name DESC, id
LIMIT 10
