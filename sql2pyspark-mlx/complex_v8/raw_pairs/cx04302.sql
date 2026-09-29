WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        CONCAT(claim_type, '#', CAST(adjuster_id AS STRING)) AS claim_type_ref,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM claims
    WHERE (approved_amount > 0.0 OR claimed_amount < 40639.7)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        claim_date,
        claimed_amount,
        status_prefix,
        SUM(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_claimed_amount,
        NTILE(3) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claim_date ASC, id) AS claim_date_tercile,
        MAX(claimed_amount) OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_claimed_amount_so_far,
        LAG(claim_date, 2) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claim_date ASC, id) AS prev2_claim_date,
        FIRST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date,
        SIZE(COLLECT_SET(claim_type) OVER (PARTITION BY YEAR(claim_date), adjuster_id)) AS distinct_claim_type_in_claim_date_year_adjuster_,
        ROUND(claimed_amount - AVG(claimed_amount) OVER w_claim_date_year_adjuster_id, 2) AS claimed_amount_vs_claim_date_year_adjuster_id_av,
        NTILE(5) OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY claim_date DESC, id) AS claim_date_quintile,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, claim_type ORDER BY claimed_amount ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_4,
        SUM(claimed_amount) OVER (ORDER BY claim_date ASC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS claimed_amount_sum_2,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY policy_id), 0), 4) AS claimed_amount_share_of_policy_id,
        ROW_NUMBER() OVER w_claim_date_year_policy_id_by_2 AS seq_no,
        LEAD(claim_date) OVER (PARTITION BY YEAR(claim_date), policy_id ORDER BY claim_date DESC, id) AS next_claim_date,
        claimed_amount - LAG(claimed_amount) OVER (PARTITION BY adjuster_id, claim_type ORDER BY claimed_amount ASC, id) AS claimed_amount_change,
        FIRST_VALUE(claimed_amount) OVER (PARTITION BY claim_type, adjuster_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claimed_amount,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY YEAR(claim_date), adjuster_id), 0), 4) AS claimed_amount_share_of_claim_date_year_adjuster
    FROM valid
    WINDOW w_claim_date_year_adjuster_id AS (PARTITION BY YEAR(claim_date), adjuster_id), w_claim_date_year_policy_id_by_2 AS (PARTITION BY YEAR(claim_date), policy_id ORDER BY claim_date DESC, id)
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        claim_date,
        policy_id_sum_claimed_amount,
        claim_date_tercile,
        max_claimed_amount_so_far,
        prev2_claim_date,
        distinct_claim_type_in_claim_date_year_adjuster_,
        claim_date_quintile,
        claimed_amount_avg_4,
        claimed_amount_share_of_policy_id,
        seq_no,
        next_claim_date,
        claimed_amount_change,
        DATEDIFF(prev2_claim_date, first_known_claim_date) AS days_first_known_claim_date_to_prev2_claim_date,
        ABS(claim_date_tercile - 7) AS claim_date_tercile_distance
    FROM with_windows
)
SELECT
    id,
    claim_date,
    policy_id_sum_claimed_amount,
    claim_date_tercile,
    max_claimed_amount_so_far,
    prev2_claim_date,
    distinct_claim_type_in_claim_date_year_adjuster_,
    claim_date_quintile,
    claimed_amount_avg_4,
    claimed_amount_share_of_policy_id,
    seq_no,
    next_claim_date,
    DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
FROM scored
WHERE days_first_known_claim_date_to_prev2_claim_date < 420
ORDER BY claim_date_quarter DESC, seq_no DESC
