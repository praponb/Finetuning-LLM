WITH prepared_holders AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        DATEDIFF(DATE '2025-12-31', joined_on) AS days_since_joined_on,
        CONCAT(segment, '#', CAST(id AS STRING)) AS segment_ref
    FROM policyholders
    WHERE (UPPER(TRIM(holder_name)) LIKE 'B%' OR joined_on BETWEEN DATE '2024-06-29' AND DATE '2025-10-17')
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM prepared_holders s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.end_date < DATE '2026-09-23'
              AND p.start_date >= DATE '2024-07-19'
        )
      AND s.region <> 'Central'
),
tagged AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(days_since_joined_on / NULLIF(risk_score, 0), 3) AS days_since_joined_on_per_risk_score,
        DAYOFWEEK(joined_on) AS joined_on_dayofweek
    FROM without_matches
)
SELECT
    id,
    holder_name,
    region,
    risk_score,
    joined_on,
    days_since_joined_on,
    segment_ref,
    days_since_joined_on_per_risk_score,
    joined_on_dayofweek
FROM tagged
ORDER BY joined_on DESC, risk_score DESC NULLS FIRST
