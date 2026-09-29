WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE product = 'HOME'
      AND end_date IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MIN(claimed_amount) AS min_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.min_claim_claimed_amount
    FROM prepared s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status_len,
        min_claim_claimed_amount,
        LENGTH(TRIM(product)) AS product_len,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m,
        COALESCE(min_claim_claimed_amount, 0.0) AS min_claim_claimed_amount_filled
    FROM with_stats
    WHERE min_claim_claimed_amount < 30040.0
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(min_claim_claimed_amount * 1.5, 2) AS min_claim_claimed_amount_adj
    FROM scored
    WHERE premium <= 4678.8
),
with_details AS (
    -- Join policyholders
    SELECT
        s.*,
        p.joined_on AS holder_joined_on,
        p.holder_name,
        p.region AS holder_region
    FROM annotated s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.joined_on BETWEEN DATE '2024-05-23' AND DATE '2025-05-01'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        min_claim_claimed_amount,
        start_date_plus_12m,
        min_claim_claimed_amount_adj,
        holder_name,
        holder_region,
        COUNT(*) OVER (PARTITION BY status_len) AS rows_in_status_len,
        ROUND(CUME_DIST() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product_len ORDER BY min_claim_claimed_amount_filled ASC), 3) AS min_claim_claimed_amount_filled_cume_dist,
        ROUND(AVG(product_len) OVER w_status_len, 2) AS status_len_avg_product_len,
        SUM(product_len) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), product_len ORDER BY min_claim_claimed_amount_filled ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_product_len
    FROM with_details
    WINDOW w_status_len AS (PARTITION BY status_len)
)
SELECT
    id,
    product,
    min_claim_claimed_amount,
    start_date_plus_12m,
    min_claim_claimed_amount_adj,
    holder_region,
    min_claim_claimed_amount_filled_cume_dist,
    status_len_avg_product_len
FROM ranked
ORDER BY min_claim_claimed_amount, start_date_plus_12m DESC NULLS FIRST
