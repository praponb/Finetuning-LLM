WITH filtered AS (
    -- Scope the adjusters rows
    SELECT
        id,
        adjuster_name,
        team,
        level,
        CASE WHEN level >= 3 THEN 'HIGH' WHEN level >= 2 THEN 'MEDIUM' ELSE 'LOW' END AS level_tier,
        CONCAT_WS('-', adjuster_name, team) AS adjuster_name_team_label,
        INITCAP(TRIM(adjuster_name)) AS adjuster_name_clean
    FROM adjusters
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.adjuster_id = s.id) AS claim_count,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.adjuster_id = s.id) AS first_claim_reported_at,
        (SELECT ROUND(AVG(c.approved_amount), 2) FROM claims c WHERE c.adjuster_id = s.id) AS avg_claim_approved_amount
    FROM filtered s
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        avg_claim_approved_amount - LAG(avg_claim_approved_amount) OVER (PARTITION BY claim_count ORDER BY avg_claim_approved_amount DESC, id) AS avg_claim_approved_amount_change,
        ROW_NUMBER() OVER (PARTITION BY team, TO_DATE(first_claim_reported_at) ORDER BY first_claim_reported_at, id) AS rn,
        ROUND(CUME_DIST() OVER w_team_first_claim_reported_at_2, 3) AS first_claim_reported_at_cume_dist
    FROM summarized_rows
    WINDOW w_team_first_claim_reported_at_2 AS (PARTITION BY team, TO_DATE(first_claim_reported_at) ORDER BY first_claim_reported_at)
),
totals AS (
    -- Summarise per group
    SELECT
        claim_count,
        level,
        rn,
        ROUND(STDDEV(avg_claim_approved_amount_change), 2) AS stddev_avg_claim_approved_amount_change,
        MAX(rn) AS max_rn
    FROM with_trends
    WHERE team NOT IN ('Property', 'Health')
    GROUP BY 1, 2, 3
    HAVING SUM(claim_count) > 2
),
annotated AS (
    -- Add calculated columns
    SELECT
        claim_count,
        level,
        rn,
        stddev_avg_claim_approved_amount_change,
        max_rn,
        FLOOR(rn / 2) * 2 AS rn_bucket,
        CASE WHEN level >= 3 THEN 'large' WHEN level >= 2 THEN 'medium' ELSE 'small' END AS level_tier,
        GREATEST(max_rn, rn) AS greatest_max_rn_rn
    FROM totals
    WHERE claim_count BETWEEN 0 AND 6
),
metrics AS (
    -- Previous / next values per key
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY claim_count) AS rows_in_claim_count,
        DENSE_RANK() OVER w_level_tier_rn_bucket_by_grea AS greatest_max_rn_rn_dense_rank,
        RANK() OVER (PARTITION BY level ORDER BY stddev_avg_claim_approved_amount_change ASC) AS stddev_avg_claim_approved_amount_change_rank,
        ROUND(AVG(greatest_max_rn_rn) OVER (PARTITION BY level_tier, rn_bucket ORDER BY greatest_max_rn_rn ASC, claim_count, level, rn ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS greatest_max_rn_rn_avg_3_prior,
        FIRST_VALUE(rn_bucket) IGNORE NULLS OVER (PARTITION BY level_tier, rn_bucket ORDER BY greatest_max_rn_rn ASC, claim_count, level, rn ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rn_bucket,
        LAG(rn_bucket, 1, 0) OVER w_level_tier_rn_bucket_by_grea_4 AS prev_rn_bucket
    FROM annotated
    WINDOW w_level_tier_rn_bucket_by_grea AS (PARTITION BY level_tier, rn_bucket ORDER BY greatest_max_rn_rn ASC), w_level_tier_rn_bucket_by_grea_4 AS (PARTITION BY level_tier, rn_bucket ORDER BY greatest_max_rn_rn ASC, claim_count, level, rn)
)
SELECT
    claim_count,
    level,
    rn_bucket,
    level_tier,
    greatest_max_rn_rn,
    rows_in_claim_count,
    stddev_avg_claim_approved_amount_change_rank,
    greatest_max_rn_rn_avg_3_prior,
    first_known_rn_bucket,
    prev_rn_bucket
FROM metrics
