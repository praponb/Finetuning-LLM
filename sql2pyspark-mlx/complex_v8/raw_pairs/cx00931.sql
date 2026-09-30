WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE (holder_id >= 15 OR product IN ('HOME', 'AUTO', 'LIFE'))
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT MAX(c.claim_date) FROM claims c WHERE c.policy_id = s.id) AS last_claim_date
    FROM filtered s
),
with_details AS (
    -- Enrich with dimension data
    SELECT s.*, p.holder_name, p.risk_score AS holder_risk_score
    FROM with_counts s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        status,
        last_claim_date,
        holder_name,
        ROUND(PERCENT_RANK() OVER w_product_by_claim_count_ties, 3) AS claim_count_percent_rank,
        MAX(holder_risk_score) OVER (PARTITION BY holder_id) AS holder_id_max_holder_risk_score,
        ROUND(claim_count / NULLIF(SUM(claim_count) OVER (PARTITION BY holder_id), 0), 4) AS claim_count_share_of_holder_id,
        ROUND(AVG(holder_risk_score) OVER w_holder_id, 2) AS holder_id_avg_holder_risk_score,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        COUNT(*) OVER (PARTITION BY product ORDER BY claim_count DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM with_details
    WINDOW w_product_by_claim_count_ties AS (PARTITION BY product ORDER BY claim_count DESC), w_holder_id AS (PARTITION BY holder_id)
),
annotated AS (
    -- Derive helper columns
    SELECT *, SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM windowed
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.status,
        s.claim_count_percent_rank,
        s.holder_id_max_holder_risk_score,
        s.claim_count_share_of_holder_id,
        s.holder_id_avg_holder_risk_score,
        s.rows_in_holder_id,
        s.running_count,
        s.product_prefix,
        p.region AS holder_region,
        p.joined_on AS holder_joined_on
    FROM annotated s
    JOIN policyholders p ON p.id = s.holder_id
),
latest_per_key AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        status,
        claim_count_percent_rank,
        holder_id_max_holder_risk_score,
        claim_count_share_of_holder_id,
        holder_id_avg_holder_risk_score,
        rows_in_holder_id,
        running_count,
        product_prefix,
        holder_region,
        holder_joined_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY holder_joined_on DESC, id DESC) AS rn
        FROM combined_details_holder
    ) ranked
    WHERE rn = 1
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    status,
    claim_count_percent_rank,
    holder_id_max_holder_risk_score,
    claim_count_share_of_holder_id,
    holder_id_avg_holder_risk_score,
    rows_in_holder_id,
    holder_joined_on,
    FLOOR(running_count / 5) * 5 AS running_count_bucket
FROM latest_per_key
ORDER BY claim_count_percent_rank
