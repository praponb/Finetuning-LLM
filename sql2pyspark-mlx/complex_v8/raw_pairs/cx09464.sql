WITH cleaned AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        ROUND(premium * 0.9, 2) AS premium_adj,
        CONCAT_WS('|', product, status) AS product_status_label
    FROM policies
    WHERE start_date BETWEEN DATE '2024-07-19' AND DATE '2025-03-24'
),
latest AS (
    -- Keep the earliest row per product
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        premium_adj,
        product_status_label
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product ORDER BY end_date, id) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, SUM(claimed_amount) AS total_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.total_claim_claimed_amount
    FROM latest s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
claim_stats_2 AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(reported_at) AS last_claim_reported_at,
        MIN(claim_date) AS first_claim_date,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        c.first_claim_date,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM summarized_rows s
    LEFT JOIN claim_stats_2 c ON c.policy_id = s.id
)
SELECT
    status,
    holder_id,
    product,
    ROUND(SUM(premium_adj), 2) AS total_premium_adj
FROM with_stats
GROUP BY GROUPING SETS ((status, holder_id, product), (status))
ORDER BY status NULLS LAST, holder_id NULLS LAST, product NULLS LAST
