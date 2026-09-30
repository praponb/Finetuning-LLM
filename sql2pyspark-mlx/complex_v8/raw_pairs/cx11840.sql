WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status
    FROM claims
    WHERE policy_id IS NOT NULL
),
combined_details_adjuster AS (
    -- Attach adjuster attributes
    SELECT s.*, a.team AS adjuster_team
    FROM valid_claims s
    JOIN adjusters a ON a.id = s.adjuster_id
    WHERE a.team IN ('Travel', 'Health')
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_min_claimed_amount,
        approved_amount - LAG(approved_amount) OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount NULLS LAST, id) AS approved_amount_change,
        MAX(approved_amount) OVER (PARTITION BY adjuster_id, policy_id) AS adjuster_id_policy_id_max_approved_amount,
        SUM(approved_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount NULLS LAST, id) AS claimed_amount_seq,
        ROUND(AVG(approved_amount) OVER (PARTITION BY status, adjuster_id ORDER BY approved_amount DESC, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS approved_amount_avg_3_prior,
        ROUND(AVG(claimed_amount) OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount ASC, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_1_prior,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        DENSE_RANK() OVER (PARTITION BY adjuster_id, policy_id ORDER BY approved_amount ASC) AS approved_amount_dense_rank,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (PARTITION BY status, adjuster_id ORDER BY claimed_amount NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER w_adjuster_id_policy_id, 2) AS approved_amount_vs_adjuster_id_policy_id_avg
    FROM combined_details_adjuster
    WINDOW w_adjuster_id_policy_id AS (PARTITION BY adjuster_id, policy_id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        adjuster_team,
        approved_amount_change,
        adjuster_id_policy_id_max_approved_amount,
        running_approved_amount,
        approved_amount_avg_3_prior,
        claimed_amount_avg_1_prior,
        approved_amount_dense_rank,
        first_known_claimed_amount,
        approved_amount_vs_adjuster_id_policy_id_avg,
        NTILE(4) OVER w_claim_type_by_claimed_amount AS claimed_amount_quartile,
        ROUND(CUME_DIST() OVER (PARTITION BY claim_type ORDER BY claimed_amount ASC), 3) AS claimed_amount_cume_dist,
        ROUND(AVG(claimed_amount_avg_1_prior) OVER (ORDER BY approved_amount_avg_3_prior DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS claimed_amount_avg_1_prior_avg_2_prior,
        MAX(approved_amount_change) OVER (PARTITION BY status) AS status_max_approved_amount_change,
        SUM(approved_amount_dense_rank) OVER w_status AS status_sum_approved_amount_dense_rank
    FROM sequenced
    WINDOW w_claim_type_by_claimed_amount AS (PARTITION BY claim_type ORDER BY claimed_amount ASC, id), w_status AS (PARTITION BY status)
),
per_group AS (
    -- Summarise per group
    SELECT
        adjuster_team,
        claimed_amount_quartile,
        COUNT(DISTINCT id) AS distinct_ids,
        SUM(CASE WHEN status IN ('REJECTED', 'PAID', 'IN_REVIEW') THEN 1 ELSE 0 END) AS n_status_in_rejected_paid_in_r,
        COUNT(*) AS n_rows,
        ROUND(AVG(claimed_amount_quartile), 2) AS avg_claimed_amount_quartile
    FROM with_trends
    WHERE approved_amount_change <= 10000.0
    GROUP BY adjuster_team, claimed_amount_quartile
    HAVING COUNT(*) >= 2
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY distinct_ids ORDER BY distinct_ids ASC NULLS LAST, adjuster_team, claimed_amount_quartile) AS distinct_ids_seq,
        ROUND(CUME_DIST() OVER (PARTITION BY distinct_ids ORDER BY distinct_ids ASC NULLS LAST), 3) AS distinct_ids_cume_dist,
        ROUND(AVG(avg_claimed_amount_quartile) OVER (PARTITION BY distinct_ids ORDER BY distinct_ids ASC NULLS LAST, adjuster_team, claimed_amount_quartile ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS avg_claimed_amount_quartile_avg_1_prior
    FROM per_group
)
SELECT
    adjuster_team,
    claimed_amount_quartile,
    n_status_in_rejected_paid_in_r,
    avg_claimed_amount_quartile,
    avg_claimed_amount_quartile_avg_1_prior,
    LOWER(TRIM(adjuster_team)) AS adjuster_team_clean,
    distinct_ids - distinct_ids_cume_dist AS distinct_ids_minus_distinct_ids_cume_dist
FROM with_windows
ORDER BY avg_claimed_amount_quartile
