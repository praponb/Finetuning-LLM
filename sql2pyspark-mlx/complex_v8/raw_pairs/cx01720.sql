WITH prepared AS (
    -- Filter policyholders
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        LENGTH(TRIM(segment)) AS segment_len
    FROM policyholders
    WHERE (region = 'East' OR holder_name IS NOT NULL)
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM prepared s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.start_date < DATE '2025-03-06'
              AND p.premium < 2945.9
        )
),
with_activity AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM qualified s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.product = 'LIFE'
              AND p.status = 'LAPSED'
        )
      AND s.segment_len BETWEEN 3 AND 13
),
tagged AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        DATEDIFF(DATE '2026-01-01', joined_on) AS days_since_joined_on
    FROM with_activity
)
SELECT
    segment_len,
    COALESCE(segment, 'ALL') AS segment,
    COUNT(*) AS row_count
FROM tagged
GROUP BY CUBE(segment_len, segment)
ORDER BY segment_len NULLS LAST, segment NULLS LAST
