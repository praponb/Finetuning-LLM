WITH scoped_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status
    FROM policies
    WHERE start_date < DATE '2025-05-28'
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS last_claim_date,
        (SELECT SUM(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_claimed_amount
    FROM scoped_policys s
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        start_date,
        status,
        total_claim_claimed_amount,
        MIN(total_claim_claimed_amount) OVER w_product_status AS product_status_min_total_claim_claimed_amount,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY product, status)) AS distinct_holder_id_in_product_status,
        LAST_VALUE(total_claim_claimed_amount) OVER (PARTITION BY status ORDER BY total_claim_claimed_amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_claim_claimed_amount,
        RANK() OVER (PARTITION BY status ORDER BY total_claim_claimed_amount) AS total_claim_claimed_amount_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY total_claim_claimed_amount), 3) AS total_claim_claimed_amount_percent_rank
    FROM with_counts
    WINDOW w_product_status AS (PARTITION BY product, status)
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM metrics s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
)
SELECT
    total_claim_claimed_amount_rank,
    distinct_holder_id_in_product_status,
    COALESCE(status, 'ALL') AS status,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list,
    MAX(product_status_min_total_claim_claimed_amount) AS max_product_status_min_total_claim_claimed_amoun,
    MAX(distinct_holder_id_in_product_status) AS max_distinct_holder_id_in_product_status
FROM without_matches
GROUP BY GROUPING SETS ((total_claim_claimed_amount_rank, distinct_holder_id_in_product_status, status), (total_claim_claimed_amount_rank), (distinct_holder_id_in_product_status), ())
ORDER BY total_claim_claimed_amount_rank NULLS LAST, distinct_holder_id_in_product_status NULLS LAST, status NULLS LAST
