WITH covered AS (
    -- Count earlier rows of the same key inside the look-back window
    SELECT
        a.id,
        a.policy_id,
        a.claim_date,
        COUNT(b.id) AS prior_rows_180d,
        SUM(b.approved_amount) AS prior_approved_amount_180d
    FROM claims a
    LEFT JOIN claims b
      ON b.policy_id = a.policy_id
     AND b.claim_date < a.claim_date
     AND b.claim_date >= DATE_SUB(a.claim_date, 180)
    GROUP BY a.id, a.policy_id, a.claim_date
),
stats AS (
    -- Group-level totals
    SELECT
        policy_id,
        ROUND(SUM(prior_approved_amount_180d) / NULLIF(SUM(prior_rows_180d), 0), 3) AS prior_approved_amount_180d_to_prior_rows_180d_ra,
        MAX(claim_date) AS last_claim_date,
        COUNT(*) AS cnt
    FROM covered
    GROUP BY 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        policy_id,
        last_claim_date,
        cnt,
        DENSE_RANK() OVER (ORDER BY last_claim_date DESC NULLS FIRST, policy_id) AS last_claim_date_dense_rank,
        LAST_VALUE(last_claim_date) OVER (ORDER BY last_claim_date DESC NULLS FIRST, policy_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_last_claim_date,
        ROW_NUMBER() OVER w_all_by_last_claim_date AS last_claim_date_seq
    FROM stats
    WINDOW w_all_by_last_claim_date AS (ORDER BY last_claim_date DESC NULLS FIRST, policy_id)
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        MIN(last_claim_date_seq) OVER (PARTITION BY cnt, policy_id) AS cnt_policy_id_min_last_claim_date_seq,
        RANK() OVER (ORDER BY last_last_claim_date ASC, policy_id) AS last_last_claim_date_rank,
        LAG(last_last_claim_date) OVER (ORDER BY last_last_claim_date ASC, policy_id) AS prev_last_last_claim_date,
        SUM(cnt) OVER (PARTITION BY cnt ORDER BY cnt DESC, policy_id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS cnt_sum_1_prior
    FROM sequenced
)
-- Final projection
SELECT
    policy_id,
    last_claim_date,
    last_claim_date_dense_rank,
    last_last_claim_date,
    prev_last_last_claim_date,
    cnt_sum_1_prior,
    DATEDIFF(last_last_claim_date, last_claim_date) AS days_last_claim_date_to_last_last_claim_date,
    LEAST(cnt, last_claim_date_seq) AS least_cnt_last_claim_date_seq
FROM with_trends
ORDER BY least_cnt_last_claim_date_seq DESC NULLS FIRST
