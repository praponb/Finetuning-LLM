WITH base_holders AS (
    -- Filter policyholders
    SELECT
        id,
        holder_name,
        region,
        risk_score,
        joined_on,
        segment,
        COALESCE(segment, 'UNKNOWN') AS segment_or_unknown,
        COALESCE(risk_score, 0.0) AS risk_score_filled,
        CASE WHEN risk_score >= 0.6 THEN 'HIGH' WHEN risk_score >= 0.3 THEN 'MEDIUM' ELSE 'LOW' END AS risk_score_tier
    FROM policyholders
    WHERE region NOT IN ('Central', 'East')
),
per_group AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(joined_on, 'yyyy-MM') AS joined_on_ym,
        region,
        segment_or_unknown,
        COUNT(DISTINCT segment) AS distinct_segments,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN segment IN ('VIP', 'SME') THEN 1 ELSE 0 END) AS n_segment_in_vip_sme,
        ROUND(STDDEV(risk_score), 2) AS stddev_risk_score
    FROM base_holders
    GROUP BY DATE_FORMAT(joined_on, 'yyyy-MM'), region, segment_or_unknown
    HAVING SUM(risk_score) > 0.2
),
tagged AS (
    -- Add calculated columns
    SELECT
        joined_on_ym,
        region,
        segment_or_unknown,
        distinct_segments,
        distinct_ids,
        stddev_risk_score,
        ABS(stddev_risk_score - 0.4) AS stddev_risk_score_distance
    FROM per_group
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(distinct_ids - AVG(distinct_ids) OVER (PARTITION BY segment_or_unknown), 2) AS distinct_ids_vs_segment_or_unknown_avg,
        ROUND(stddev_risk_score_distance - AVG(stddev_risk_score_distance) OVER w_segment_or_unknown, 2) AS stddev_risk_score_distance_vs_segment_or_unknown,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_ids, region ORDER BY distinct_ids DESC), 3) AS distinct_ids_percent_rank,
        ROUND(PERCENT_RANK() OVER w_distinct_ids_region_by_disti_2, 3) AS distinct_ids_percent_rank_2,
        NTILE(3) OVER (PARTITION BY distinct_ids, region ORDER BY distinct_ids DESC, joined_on_ym, region, segment_or_unknown) AS distinct_ids_tercile,
        ROUND(AVG(stddev_risk_score_distance) OVER (PARTITION BY segment_or_unknown), 2) AS segment_or_unknown_avg_stddev_risk_score_distanc
    FROM tagged
    WINDOW w_segment_or_unknown AS (PARTITION BY segment_or_unknown), w_distinct_ids_region_by_disti_2 AS (PARTITION BY distinct_ids, region ORDER BY distinct_ids DESC, joined_on_ym, region, segment_or_unknown)
),
tagged_2 AS (
    -- Label and score rows
    SELECT
        joined_on_ym,
        region,
        segment_or_unknown,
        distinct_segments,
        distinct_ids,
        stddev_risk_score,
        stddev_risk_score_distance,
        distinct_ids_vs_segment_or_unknown_avg,
        stddev_risk_score_distance_vs_segment_or_unknown,
        distinct_ids_tercile,
        segment_or_unknown_avg_stddev_risk_score_distanc,
        COALESCE(region, 'UNKNOWN') AS region_or_unknown
    FROM sequenced
)
-- Final projection
SELECT
    joined_on_ym,
    region,
    segment_or_unknown,
    distinct_segments,
    distinct_ids,
    stddev_risk_score,
    stddev_risk_score_distance,
    distinct_ids_vs_segment_or_unknown_avg,
    stddev_risk_score_distance_vs_segment_or_unknown,
    distinct_ids_tercile,
    segment_or_unknown_avg_stddev_risk_score_distanc,
    CONCAT(segment_or_unknown, '#', CAST(distinct_segments AS STRING)) AS segment_or_unknown_ref
FROM tagged_2
ORDER BY region DESC
