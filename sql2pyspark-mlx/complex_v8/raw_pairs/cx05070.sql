WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status
    FROM policies
),
with_children_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        c.id AS claim_id,
        c.claim_date,
        c.reported_at AS claim_reported_at,
        c.claim_type,
        c.status AS claim_status
    FROM valid_policys s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.claim_type <> 'theft'
),
deduped AS (
    -- Keep the latest row per claim_type, claim_status
    SELECT
        id,
        holder_id,
        product,
        start_date,
        claim_id,
        claim_date,
        claim_reported_at,
        claim_type,
        claim_status
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, claim_status ORDER BY start_date DESC, id DESC, claim_id DESC) AS row_num
        FROM with_children_claims
    ) ranked
    WHERE row_num = 1
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        claim_id,
        claim_date,
        claim_status,
        LOWER(TRIM(product)) AS product_clean,
        LENGTH(TRIM(product)) AS product_len
    FROM deduped
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        claim_id,
        claim_date,
        claim_status,
        product_clean,
        product_len,
        DATEDIFF(DATE '2026-01-01', claim_date) AS days_since_claim_date,
        DATE_ADD(claim_date, 365) AS claim_date_plus_365d,
        LENGTH(TRIM(claim_status)) AS claim_status_len
    FROM scored
),
with_parent_claim AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.claim_id,
        s.claim_date,
        s.claim_status,
        s.product_clean,
        s.product_len,
        s.days_since_claim_date,
        s.claim_status_len,
        c.reported_at AS claim_reported_at,
        c.claim_type
    FROM annotated s
    JOIN claims c ON c.id = s.claim_id AND c.status = 'APPROVED'
),
latest_per_key AS (
    -- Keep the latest row per product, holder_id
    SELECT
        id,
        holder_id,
        product,
        claim_id,
        claim_date,
        claim_status,
        product_clean,
        product_len,
        days_since_claim_date,
        claim_status_len,
        claim_reported_at,
        claim_type
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY product_len DESC, id DESC, claim_id DESC) AS recency_rank
        FROM with_parent_claim
    ) ranked
    WHERE recency_rank = 1
)
SELECT
    id,
    holder_id,
    product,
    claim_id,
    claim_date,
    claim_status,
    product_len,
    days_since_claim_date,
    claim_status_len,
    claim_type,
    ADD_MONTHS(claim_date, 6) AS claim_date_plus_6m,
    CONCAT_WS('-', product, product_clean) AS product_product_clean_label
FROM latest_per_key
ORDER BY claim_status DESC, id DESC
