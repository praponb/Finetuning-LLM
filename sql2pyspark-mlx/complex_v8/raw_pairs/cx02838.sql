WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount
    FROM claims
    WHERE claimed_amount <= 41878.5
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY policy_id ORDER BY claimed_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        COUNT(*) OVER (PARTITION BY policy_id) AS rows_in_policy_id,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        MAX(claimed_amount) OVER w_policy_id AS policy_id_max_claimed_amount,
        ROW_NUMBER() OVER (PARTITION BY adjuster_id ORDER BY approved_amount, id) AS approved_amount_seq,
        RANK() OVER (ORDER BY approved_amount, id) AS approved_amount_rank,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id, 0), 4) AS claimed_amount_share_of_policy_id,
        MIN(approved_amount) OVER (PARTITION BY policy_id) AS policy_id_min_approved_amount,
        ROUND(approved_amount - AVG(approved_amount) OVER w_policy_id, 2) AS approved_amount_vs_policy_id_avg,
        ROUND(AVG(claimed_amount) OVER w_policy_id, 2) AS policy_id_avg_claimed_amount,
        ROUND(claimed_amount - AVG(claimed_amount) OVER (PARTITION BY policy_id), 2) AS claimed_amount_vs_policy_id_avg,
        FIRST_VALUE(claimed_amount) IGNORE NULLS OVER (ORDER BY approved_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claimed_amount
    FROM cleaned_claims
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
top_rows AS (
    -- Top 5 per group
    SELECT *
    FROM windowed
    WHERE approved_amount_rank <= 5
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(claimed_amount) OVER (PARTITION BY YEAR(second_claim_date)) AS second_claim_date_year_max_claimed_amount,
        ROUND(PERCENT_RANK() OVER w_adjuster_id_approved_amount, 3) AS policy_id_avg_claimed_amount_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY policy_id ORDER BY claimed_amount NULLS LAST, id) AS seq_no,
        MIN(policy_id_avg_claimed_amount) OVER (ORDER BY second_claim_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_policy_id_avg_claimed_amount_so_far,
        ROUND(CUME_DIST() OVER (PARTITION BY policy_id ORDER BY claimed_amount NULLS LAST), 3) AS claimed_amount_cume_dist,
        ROUND(last_known_approved_amount - AVG(last_known_approved_amount) OVER (PARTITION BY approved_amount_seq, adjuster_id), 2) AS last_known_approved_amount_vs_approved_amount_se
    FROM top_rows
    WINDOW w_adjuster_id_approved_amount AS (PARTITION BY adjuster_id, approved_amount_seq ORDER BY policy_id_avg_claimed_amount DESC)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    claimed_amount,
    rows_in_policy_id,
    last_known_approved_amount,
    policy_id_max_claimed_amount,
    approved_amount_seq,
    approved_amount_rank,
    claimed_amount_share_of_policy_id,
    policy_id_min_approved_amount,
    approved_amount_vs_policy_id_avg,
    policy_id_avg_claimed_amount,
    first_known_claimed_amount,
    second_claim_date_year_max_claimed_amount,
    seq_no,
    min_policy_id_avg_claimed_amount_so_far,
    claimed_amount_cume_dist,
    last_known_approved_amount_vs_approved_amount_se,
    DATE_FORMAT(second_claim_date, 'yyyy-MM') AS second_claim_date_label
FROM with_trends
WHERE second_claim_date IS NOT NULL
ORDER BY claimed_amount DESC, last_known_approved_amount_vs_approved_amount_se DESC
