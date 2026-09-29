WITH matched AS (
    -- Claims that fall inside the policy coverage period
    SELECT
        c.id,
        c.policy_id,
        c.claim_date,
        c.claimed_amount,
        c.adjuster_id,
        c.approved_amount,
        p.product,
        p.premium
    FROM claims c
    LEFT JOIN policies p
      ON p.id = c.policy_id
     AND c.claim_date BETWEEN p.start_date AND COALESCE(p.end_date, DATE '2099-12-31')
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        approved_amount,
        product,
        INITCAP(TRIM(product)) AS product_clean
    FROM matched
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FILTER (WHERE c.status IS NOT NULL) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.policy_id) AS total_claim_approved_amount,
        (SELECT MIN(c.reported_at) FROM claims c WHERE c.policy_id = s.policy_id) AS first_claim_reported_at
    FROM derived s
),
claim_stats_2 AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, COUNT(*) AS claim_count, COUNT(*) AS claim_count_2
    FROM claims
    GROUP BY adjuster_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        COALESCE(c.claim_count, 0) AS claim_count,
        COALESCE(c.claim_count_2, 0) AS claim_count_2
    FROM with_counts s
    LEFT JOIN claim_stats_2 c ON c.adjuster_id = s.adjuster_id
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, metric_name, reading
FROM (
    SELECT id, approved_amount, total_claim_approved_amount
    FROM summarized_rows
) src
UNPIVOT (
    reading FOR metric_name IN (approved_amount AS m_approved_amount, total_claim_approved_amount AS m_total_claim_approved_amount)
) unp
