WITH scoped_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(claimed_amount) AS max_claim_claimed_amount,
        MAX(reported_at) AS last_claim_reported_at
    FROM claims
    GROUP BY policy_id
),
summarized_rows AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_claimed_amount,
        c.last_claim_reported_at
    FROM scoped_policys s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
filtered AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM summarized_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.start_date >= DATE '2024-10-22'
              AND p.status IN ('EXPIRED', 'LAPSED', 'ACTIVE')
        )
),
latest_per_key AS (
    -- Keep the earliest row per holder_id, product
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        max_claim_claimed_amount,
        last_claim_reported_at
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY max_claim_claimed_amount, id) AS rn
        FROM filtered
    ) ranked
    WHERE rn = 1
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY last_claim_reported_at ASC), 3) AS last_claim_reported_at_cume_dist,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        LAG(end_date) OVER (PARTITION BY product, holder_id ORDER BY last_claim_reported_at DESC NULLS FIRST, id) AS prev_end_date,
        LEAD(last_claim_reported_at) OVER (PARTITION BY holder_id ORDER BY last_claim_reported_at ASC, id) AS next_last_claim_reported_at,
        ROUND(premium / NULLIF(SUM(premium) OVER w_product, 0), 4) AS premium_share_of_product
    FROM latest_per_key
    WINDOW w_product AS (PARTITION BY product)
),
latest_per_key_2 AS (
    -- Keep the latest row per product, status
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        max_claim_claimed_amount,
        last_claim_reported_at,
        last_claim_reported_at_cume_dist,
        rows_in_product,
        prev_end_date,
        next_last_claim_reported_at,
        premium_share_of_product
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY last_claim_reported_at_cume_dist DESC, id DESC) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
),
holder_id_stats AS (
    -- Max premium per holder_id
    SELECT holder_id, MAX(premium) AS max_premium
    FROM latest_per_key_2
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.max_premium,
        ROUND(s.premium - g.max_premium, 2) AS premium_minus_max
    FROM latest_per_key_2 s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.premium = g.max_premium
)
SELECT
    id,
    holder_id,
    product,
    status,
    max_claim_claimed_amount,
    last_claim_reported_at,
    last_claim_reported_at_cume_dist,
    prev_end_date,
    next_last_claim_reported_at,
    premium_share_of_product,
    FLOOR(max_claim_claimed_amount / 10000) * 10000 AS max_claim_claimed_amount_bucket
FROM above_average
ORDER BY last_claim_reported_at_cume_dist DESC NULLS FIRST, max_claim_claimed_amount
