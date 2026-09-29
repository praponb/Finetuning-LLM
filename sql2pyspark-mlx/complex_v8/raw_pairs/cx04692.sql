WITH scoped_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-24 12:00:00'
      AND adjuster_id >= 5
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM scoped_claims
    WHERE approved_amount > (SELECT AVG(approved_amount) FROM scoped_claims)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        claim_type,
        claimed_amount,
        approved_amount,
        claim_date_week,
        claimed_amount - LAG(claimed_amount) OVER (ORDER BY claimed_amount NULLS LAST, id) AS claimed_amount_change,
        MIN(claimed_amount) OVER (PARTITION BY policy_id, MONTH(claim_date_week)) AS policy_id_claim_date_week_month_min_claimed_amou,
        LAST_VALUE(claimed_amount) OVER (ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        COUNT(*) OVER (PARTITION BY policy_id, MONTH(claim_date_week)) AS rows_in_policy_id_claim_date_week_month,
        ROUND(AVG(approved_amount) OVER w_policy_id_claim_date_week_mo, 2) AS policy_id_claim_date_week_month_avg_approved_amo,
        ROUND(PERCENT_RANK() OVER (ORDER BY claimed_amount NULLS LAST), 3) AS claimed_amount_percent_rank
    FROM without_matches
    WINDOW w_policy_id_claim_date_week_mo AS (PARTITION BY policy_id, MONTH(claim_date_week))
),
without_matches_2 AS (
    -- Compare against the overall avg
    SELECT *
    FROM with_trends
    WHERE claimed_amount_change > (SELECT AVG(claimed_amount_change) FROM with_trends)
),
joined_policy AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.policy_id,
        s.claim_type,
        s.claimed_amount,
        s.approved_amount,
        s.claim_date_week,
        s.claimed_amount_change,
        s.rows_in_policy_id_claim_date_week_month,
        s.policy_id_claim_date_week_month_avg_approved_amo,
        s.claimed_amount_percent_rank,
        p.end_date AS policy_end_date,
        p.start_date AS policy_start_date,
        p.holder_id AS policy_holder_id,
        po.region AS holder_region
    FROM without_matches_2 s
    JOIN policies p ON p.id = s.policy_id
    JOIN policyholders po ON po.id = p.holder_id
),
policy_stats AS (
    -- Per-holder policies metrics
    SELECT
        holder_id,
        COUNT(*) AS policy_count,
        MAX(end_date) AS last_policy_end_date
    FROM policies
    GROUP BY holder_id
),
profiled AS (
    -- Attach policies metrics
    SELECT
        s.*,
        COALESCE(p.policy_count, 0) AS policy_count,
        p.last_policy_end_date
    FROM joined_policy s
    LEFT JOIN policy_stats p ON p.holder_id = s.policy_holder_id
),
holder_region_stats AS (
    -- Median claimed_amount per holder_region
    SELECT holder_region, PERCENTILE_APPROX(claimed_amount, 0.5) AS median_claimed_amount
    FROM profiled
    GROUP BY holder_region
),
vs_group AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.median_claimed_amount,
        ROUND(s.claimed_amount - g.median_claimed_amount, 2) AS claimed_amount_minus_median
    FROM profiled s
    JOIN holder_region_stats g ON g.holder_region = s.holder_region
    WHERE s.claimed_amount >= g.median_claimed_amount
)
SELECT
    COALESCE(holder_region, 'ALL') AS holder_region,
    COALESCE(claim_type, 'ALL') AS claim_type,
    policy_id,
    GROUPING_ID() AS gid,
    COUNT(*) FILTER (WHERE holder_region <> 'Central') AS n_holder_region_central,
    COUNT(*) AS row_count,
    ROUND(STDDEV(claimed_amount_change), 2) AS stddev_claimed_amount_change
FROM vs_group
GROUP BY ROLLUP(holder_region, claim_type, policy_id)
ORDER BY holder_region NULLS LAST, claim_type NULLS LAST, policy_id NULLS LAST
