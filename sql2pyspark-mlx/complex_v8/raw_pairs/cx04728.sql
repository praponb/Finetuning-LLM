WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        region,
        risk_score,
        joined_on,
        segment,
        QUARTER(joined_on) AS joined_on_quarter,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        DATE_ADD(joined_on, 90) AS joined_on_plus_90d
    FROM policyholders
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(joined_on_quarter) OVER (PARTITION BY segment, YEAR(joined_on) ORDER BY joined_on_plus_90d, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS joined_on_quarter_sum_3,
        ROUND(PERCENT_RANK() OVER (PARTITION BY segment, YEAR(joined_on) ORDER BY joined_on_plus_90d), 3) AS joined_on_plus_90d_percent_rank,
        SUM(joined_on_quarter) OVER (PARTITION BY segment, joined_on_quarter) AS segment_joined_on_quarter_sum_joined_on_quarter,
        LAG(joined_on) OVER w_joined_on_quarter_joined_on AS prev_joined_on
    FROM scoped
    WINDOW w_joined_on_quarter_joined_on AS (PARTITION BY joined_on_quarter, YEAR(joined_on) ORDER BY joined_on_quarter ASC, id)
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.id
              AND p.status = 'LAPSED'
        )
      AND s.prev_joined_on >= DATE '2024-03-22'
)
SELECT *
FROM (
    SELECT region, segment, risk_score
    FROM filtered
) src
PIVOT (
    AVG(risk_score)
    FOR segment IN ('VIP', 'RETAIL', 'SME')
)
ORDER BY region
