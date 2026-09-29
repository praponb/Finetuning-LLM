WITH prepared_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        INITCAP(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE claimed_amount > 13635.1
      AND status NOT IN ('OPEN', 'PAID')
),
ranked AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY claim_type, status ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date_2,
        NTILE(4) OVER (PARTITION BY claim_type, status ORDER BY claim_date, id) AS claim_date_quartile,
        LAST_VALUE(claim_date) OVER (ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date,
        LAG(claim_date) OVER (PARTITION BY claim_type, status ORDER BY claim_date, id) AS prev_claim_date,
        ROW_NUMBER() OVER (PARTITION BY claim_type, status ORDER BY claim_date, id) AS claim_date_seq,
        LAG(claim_date, 3) OVER w_claim_type_status_by_claim_d_2 AS prev3_claim_date,
        RANK() OVER (PARTITION BY claim_type ORDER BY claim_date) AS claim_date_rank,
        LAST_VALUE(claim_date) IGNORE NULLS OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_claim_date_3,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', claim_date), adjuster_id ORDER BY claim_date ASC, id), 3) AS claim_date_cume_dist,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type, status ORDER BY claim_date), 3) AS claim_date_cume_dist_2,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_all_by_claim_date) AS days_since_prev_claim_date,
        ROW_NUMBER() OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date DESC, id) AS rn,
        LAST_VALUE(claim_date) OVER (PARTITION BY policy_id, DATE_TRUNC('MONTH', claim_date) ORDER BY claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date_2,
        NTILE(10) OVER (PARTITION BY claim_type, status ORDER BY claim_date, id) AS claim_date_decile
    FROM prepared_claims
    WINDOW w_claim_type_status_by_claim_d_2 AS (PARTITION BY claim_type, status ORDER BY claim_date, id), w_all_by_claim_date AS (ORDER BY claim_date ASC, id)
),
leaders AS (
    -- Top 2 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        claim_type_clean,
        last_known_claim_date,
        second_claim_date,
        last_known_claim_date_2,
        claim_date_quartile,
        last_claim_date,
        prev_claim_date,
        prev3_claim_date,
        claim_date_rank,
        last_known_claim_date_3,
        claim_date_cume_dist,
        claim_date_cume_dist_2,
        days_since_prev_claim_date,
        rn,
        last_claim_date_2,
        claim_date_decile
    FROM ranked
    WHERE claim_date_seq <= 2
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        rn - LAG(rn) OVER (PARTITION BY claim_date_quartile, YEAR(prev3_claim_date) ORDER BY claim_date_rank, id) AS rn_change,
        ROUND(AVG(claim_date_decile) OVER (PARTITION BY rn, claim_date_quartile ORDER BY claim_date_decile NULLS LAST, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), 2) AS claim_date_decile_avg_6_prior,
        ROW_NUMBER() OVER (PARTITION BY claim_date_quartile, YEAR(prev3_claim_date) ORDER BY claim_date_rank, id) AS rn_2,
        FIRST_VALUE(claim_date_cume_dist_2) OVER (PARTITION BY claim_date_quartile, YEAR(prev3_claim_date) ORDER BY claim_date_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_cume_dist_2,
        ROUND(CUME_DIST() OVER w_claim_date_quartile_prev3_cl_3, 3) AS claim_date_rank_cume_dist,
        NTH_VALUE(last_claim_date_2, 2) OVER (PARTITION BY claim_date_quartile, YEAR(prev3_claim_date) ORDER BY claim_date_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_last_claim_date_2,
        days_since_prev_claim_date - LAG(days_since_prev_claim_date) OVER (PARTITION BY rn, claim_date_quartile ORDER BY claim_date_decile NULLS LAST, id) AS days_since_prev_claim_date_change,
        NTILE(3) OVER w_claim_date_quartile_prev3_cl AS claim_date_rank_tercile
    FROM leaders
    WINDOW w_claim_date_quartile_prev3_cl_3 AS (PARTITION BY claim_date_quartile, YEAR(prev3_claim_date) ORDER BY claim_date_rank), w_claim_date_quartile_prev3_cl AS (PARTITION BY claim_date_quartile, YEAR(prev3_claim_date) ORDER BY claim_date_rank, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_type,
    status,
    claim_type_clean,
    second_claim_date,
    claim_date_quartile,
    last_claim_date,
    prev_claim_date,
    prev3_claim_date,
    claim_date_rank,
    last_known_claim_date_3,
    claim_date_cume_dist,
    claim_date_cume_dist_2,
    days_since_prev_claim_date,
    claim_date_decile,
    rn_change,
    claim_date_decile_avg_6_prior,
    rn_2,
    first_claim_date_cume_dist_2,
    claim_date_rank_cume_dist,
    second_last_claim_date_2,
    claim_date_rank_tercile,
    INITCAP(TRIM(claim_type_clean)) AS claim_type_clean_clean,
    LENGTH(TRIM(claim_type_clean)) AS claim_type_clean_len
FROM sequenced
WHERE claim_date_rank_cume_dist <= 0.8
ORDER BY rn_2 NULLS LAST, days_since_prev_claim_date NULLS LAST, id
LIMIT 10
