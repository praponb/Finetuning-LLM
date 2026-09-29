WITH scoped_claims AS (
    -- Filter claims
    SELECT *
    FROM claims
),
joined_policy AS (
    -- Join policies
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.claimed_amount,
        s.reported_at,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id
    FROM scoped_claims s
    JOIN policies p ON p.id = s.policy_id
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        policy_start_date,
        RANK() OVER (PARTITION BY policy_status ORDER BY reported_at DESC, id) AS reported_at_rank,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY adjuster_id, policy_holder_id ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY policy_status, TO_DATE(reported_at) ORDER BY reported_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_2_prior,
        ROUND(PERCENT_RANK() OVER (PARTITION BY policy_status ORDER BY reported_at DESC), 3) AS reported_at_percent_rank,
        NTILE(4) OVER w_adjuster_id_policy_holder_id_2 AS reported_at_quartile,
        COUNT(*) OVER (PARTITION BY YEAR(policy_start_date), adjuster_id) AS rows_in_policy_start_date_year_adjuster_id,
        LAG(claim_date) OVER (PARTITION BY adjuster_id, policy_holder_id ORDER BY reported_at, id) AS prev_claim_date,
        RANK() OVER (PARTITION BY adjuster_id, policy_holder_id ORDER BY reported_at, id) AS reported_at_rank_2,
        ROW_NUMBER() OVER (PARTITION BY policy_status, TO_DATE(reported_at) ORDER BY reported_at, id) AS rn,
        NTILE(5) OVER (PARTITION BY adjuster_id, policy_holder_id ORDER BY reported_at, id) AS reported_at_quintile,
        LAG(reported_at) OVER (PARTITION BY policy_status ORDER BY reported_at DESC, id) AS prev_reported_at,
        MAX(claimed_amount) OVER (PARTITION BY policy_status ORDER BY reported_at DESC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS claimed_amount_max_1_prior,
        ROUND(PERCENT_RANK() OVER w_policy_status_by_reported_at, 3) AS reported_at_percent_rank_2
    FROM joined_policy
    WINDOW w_adjuster_id_policy_holder_id_2 AS (PARTITION BY adjuster_id, policy_holder_id ORDER BY reported_at, id), w_policy_status_by_reported_at AS (PARTITION BY policy_status ORDER BY reported_at DESC, id)
),
with_windows_2 AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_type,
        policy_start_date,
        reported_at_rank,
        first_known_claim_date,
        claimed_amount_avg_2_prior,
        reported_at_quartile,
        rows_in_policy_start_date_year_adjuster_id,
        prev_claim_date,
        rn,
        prev_reported_at,
        claimed_amount_max_1_prior,
        reported_at_percent_rank_2,
        NTILE(4) OVER (PARTITION BY rn ORDER BY prev_claim_date, id) AS prev_claim_date_quartile,
        reported_at_percent_rank_2 - LAG(reported_at_percent_rank_2) OVER (ORDER BY first_known_claim_date DESC NULLS FIRST, id) AS reported_at_percent_rank_2_change,
        ROUND(reported_at_rank_2 - AVG(reported_at_rank_2) OVER w_reported_at_quintile, 2) AS reported_at_rank_2_vs_reported_at_quintile_avg,
        SUM(reported_at_rank_2) OVER (PARTITION BY reported_at_quintile) AS reported_at_quintile_sum_reported_at_rank_2,
        RANK() OVER (PARTITION BY rn ORDER BY prev_claim_date) AS prev_claim_date_rank,
        ROUND(AVG(reported_at_rank) OVER (ORDER BY first_known_claim_date DESC NULLS FIRST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS reported_at_rank_avg_2
    FROM with_windows
    WINDOW w_reported_at_quintile AS (PARTITION BY reported_at_quintile)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(prev_claim_date) AS prev_claim_date_year,
        reported_at_rank,
        COUNT(DISTINCT claim_type) AS distinct_claim_types,
        COUNT(*) AS n_rows,
        SUM(CASE WHEN claim_type = 'flood' THEN 1 ELSE 0 END) AS n_claim_type_flood,
        SUM(prev_claim_date_quartile) FILTER (WHERE claim_type <> 'flood') AS prev_claim_date_quartile_claim_type_flood,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(*) FILTER (WHERE claim_type = 'fire') AS n_claim_type_fire
    FROM with_windows_2
    GROUP BY YEAR(prev_claim_date), reported_at_rank
    HAVING COUNT(*) >= 2
)
-- Final projection
SELECT
    prev_claim_date_year,
    reported_at_rank,
    n_claim_type_flood,
    prev_claim_date_quartile_claim_type_flood,
    distinct_ids,
    n_claim_type_fire
FROM summary
ORDER BY n_claim_type_fire
