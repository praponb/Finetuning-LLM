WITH base_holders AS (
    -- Scope the policyholders rows
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        segment
    FROM policyholders
    WHERE (risk_score BETWEEN 0.1 AND 0.9 OR YEAR(joined_on) = 2024)
),
stats AS (
    -- Summarise per group
    SELECT
        region,
        SUM(risk_score) FILTER (WHERE segment <> 'SME') AS risk_score_segment_sme,
        ROUND(STDDEV(risk_score), 2) AS stddev_risk_score,
        MIN(risk_score) AS min_risk_score,
        ROUND(SUM(risk_score), 2) AS total_risk_score,
        SUM(CASE WHEN segment IN ('SME', 'VIP') THEN risk_score ELSE 0 END) AS risk_score_segment_in_sme_vip
    FROM base_holders
    GROUP BY region
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        region,
        risk_score_segment_sme,
        stddev_risk_score,
        total_risk_score,
        NTH_VALUE(risk_score_segment_sme, 2) OVER (PARTITION BY region ORDER BY stddev_risk_score NULLS LAST, region ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_risk_score_segment_sme,
        ROUND(PERCENT_RANK() OVER w_region_by_min_risk_score_tie, 3) AS min_risk_score_percent_rank
    FROM stats
    WINDOW w_region_by_min_risk_score_tie AS (PARTITION BY region ORDER BY min_risk_score ASC)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown,
        CASE WHEN region IN ('Central', 'West') THEN 'group_a' WHEN region = 'North' THEN 'group_b' ELSE 'other' END AS region_group
    FROM sequenced
),
filtered AS (
    -- Compare against the overall avg
    SELECT *
    FROM labeled
    WHERE stddev_risk_score > (SELECT AVG(stddev_risk_score) FROM labeled)
)
SELECT *
FROM (
    SELECT region_or_unknown, region_group, region, risk_score_segment_sme
    FROM filtered
) src
PIVOT (
    MAX(risk_score_segment_sme)
    FOR region IN ('Central', 'North')
)
ORDER BY region_or_unknown, region_group
