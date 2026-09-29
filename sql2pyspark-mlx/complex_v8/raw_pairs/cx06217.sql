WITH scoped_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
    FROM claims
    WHERE claimed_amount < 44991.5
      AND adjuster_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        claim_date,
        ROUND(AVG(approved_amount) OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS approved_amount_avg_4,
        LAST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claimed_amount,
        ROUND(PERCENT_RANK() OVER w_all_by_claimed_amount_ties, 3) AS claimed_amount_percent_rank,
        DENSE_RANK() OVER w_all_by_claim_date AS claim_date_dense_rank,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_max_approved_amount,
        SUM(claimed_amount) OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 2) AS claimed_amount_vs_adjuster_id_policy_id_avg,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        approved_amount - LAG(approved_amount) OVER w_all_by_claim_date AS approved_amount_change,
        NTILE(5) OVER (ORDER BY claim_date ASC, id) AS claim_date_quintile,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY adjuster_id, policy_id), 0), 4) AS approved_amount_share_of_adjuster_id_policy_id,
        LAG(claimed_amount, 3) OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date ASC, id) AS prev3_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id), 0), 4) AS claimed_amount_share_of_adjuster_id_policy_id,
        COUNT(*) OVER (PARTITION BY adjuster_id, policy_id) AS rows_in_adjuster_id_policy_id
    FROM scoped_claims
    WINDOW w_all_by_claimed_amount_ties AS (ORDER BY claimed_amount ASC), w_all_by_claim_date AS (ORDER BY claim_date ASC, id)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_TRUNC('YEAR', claim_date) AS claim_date_year,
        FLOOR(prev3_claimed_amount / 10000) * 10000 AS prev3_claimed_amount_bucket,
        DAYOFWEEK(claim_date) AS claim_date_dayofweek
    FROM metrics
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        claim_date_dayofweek,
        policy_id,
        claim_date_quintile,
        MAX(claim_date_year) AS last_claim_date_year,
        COUNT(DISTINCT policy_id) AS distinct_policy_ids,
        MIN(claim_date_year) AS first_claim_date_year,
        ROUND(STDDEV(last_known_claimed_amount), 2) AS stddev_last_known_claimed_amount,
        ROUND(STDDEV(approved_amount_change), 2) AS stddev_approved_amount_change,
        COUNT(*) AS row_count
    FROM labeled
    GROUP BY claim_date_dayofweek, policy_id, claim_date_quintile
)
-- Final projection
SELECT
    claim_date_dayofweek,
    policy_id,
    last_claim_date_year,
    stddev_last_known_claimed_amount,
    stddev_approved_amount_change,
    row_count
FROM stats
WHERE stddev_last_known_claimed_amount < 42500.0
ORDER BY last_claim_date_year
