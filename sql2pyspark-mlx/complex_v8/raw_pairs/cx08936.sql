WITH valid_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        SUBSTRING(TRIM(region), 1, 3) AS region_prefix,
        SUBSTRING(TRIM(holder_name), 1, 3) AS holder_name_prefix
    FROM policyholders
    WHERE joined_on BETWEEN DATE '2024-02-04' AND DATE '2025-10-17'
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM valid_holders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.status IN ('LAPSED', 'CANCELLED')
              AND YEAR(p.start_date) = 2024
        )
      AND LOWER(s.holder_name) LIKE '%er%'
),
without_matches AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE risk_score < (SELECT MAX(risk_score) FROM filtered)
),
unique_rows AS (
    -- Keep the earliest row per segment, region
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        region_prefix,
        holder_name_prefix
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY segment, region ORDER BY risk_score, id) AS rn
        FROM without_matches
    ) ranked
    WHERE rn = 1
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        DATE_TRUNC('QUARTER', joined_on) AS joined_on_quarter,
        LOWER(REGEXP_REPLACE(TRIM(segment), ' ', '_')) AS segment_slug
    FROM unique_rows
    WHERE holder_name_prefix IS NOT NULL
),
screened AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM labeled s
    WHERE s.id IN (
            SELECT p.holder_id
            FROM policies p
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT joined_on, joined_on_quarter, segment, risk_score_filled
    FROM screened
) src
PIVOT (
    AVG(risk_score_filled) AS avg_v, MAX(risk_score_filled) AS max_v
    FOR segment IN ('SME', 'VIP', 'RETAIL')
)
ORDER BY joined_on, joined_on_quarter
