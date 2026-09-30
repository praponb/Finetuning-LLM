WITH filtered_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        ABS(approved_amount - 16404.6) AS approved_amount_distance
    FROM claims
    WHERE (claimed_amount > 19347.2 OR policy_id IS NOT NULL)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_type,
        status_prefix,
        approved_amount_distance,
        ROUND(approved_amount_distance - AVG(approved_amount_distance) OVER (PARTITION BY status, claim_type), 2) AS approved_amount_distance_vs_status_claim_type_av,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount_distance), 3) AS approved_amount_distance_cume_dist,
        LAST_VALUE(claimed_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claimed_amount,
        SUM(approved_amount_distance) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS approved_amount_distance_sum_4,
        SUM(approved_amount_distance) OVER (PARTITION BY MONTH(claim_date) ORDER BY approved_amount ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS approved_amount_distance_sum_1_prior,
        ROUND(CUME_DIST() OVER (ORDER BY approved_amount_distance, id), 3) AS approved_amount_distance_cume_dist_2,
        NTILE(5) OVER (PARTITION BY MONTH(claim_date) ORDER BY approved_amount ASC, id) AS approved_amount_quintile,
        NTILE(5) OVER (PARTITION BY policy_id ORDER BY claim_date DESC, id) AS claim_date_quintile,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), policy_id), 0), 4) AS approved_amount_share_of_claim_date_month_policy
    FROM filtered_claims
)
-- Final projection
SELECT
    id,
    adjuster_id,
    claim_type,
    approved_amount_distance,
    approved_amount_distance_vs_status_claim_type_av,
    approved_amount_distance_cume_dist,
    last_claimed_amount,
    approved_amount_distance_sum_1_prior,
    approved_amount_quintile,
    claim_date_quintile,
    approved_amount_share_of_claim_date_month_policy,
    GREATEST(approved_amount_distance, claim_date_quintile) AS greatest_approved_amount_distance_claim_date_qui,
    SUBSTRING(TRIM(status_prefix), 1, 3) AS status_prefix_prefix
FROM sequenced
ORDER BY last_claimed_amount DESC
