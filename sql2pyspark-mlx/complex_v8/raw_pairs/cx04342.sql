WITH filtered_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        risk_score,
        joined_on,
        segment,
        CASE WHEN segment IN ('RETAIL', 'VIP') THEN 'group_a' WHEN segment = 'SME' THEN 'group_b' ELSE 'other' END AS segment_group
    FROM policyholders
    WHERE LENGTH(TRIM(holder_name)) >= 6
      AND YEAR(joined_on) = 2024
),
tagged AS (
    -- Derive helper columns
    SELECT
        id,
        risk_score,
        segment,
        segment_group,
        DATE_ADD(joined_on, 30) AS joined_on_plus_30d,
        DATE_FORMAT(joined_on, 'MMM yyyy') AS joined_on_label
    FROM filtered_holders
)
-- Subtotals with ROLLUP
SELECT
    segment,
    segment_group,
    COUNT(*) AS n_rows,
    ROUND(100 * COUNT(CASE WHEN segment <> 'SME' THEN 1 END) / COUNT(*), 1) AS pct_segment_sme
FROM tagged
GROUP BY ROLLUP(segment, segment_group)
ORDER BY segment NULLS LAST, segment_group NULLS LAST
