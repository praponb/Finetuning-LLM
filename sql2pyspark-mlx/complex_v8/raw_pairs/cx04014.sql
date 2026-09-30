WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE end_date IS NOT NULL
      AND product = 'LIFE'
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        product_ref,
        status_len,
        LEAST(status_len, premium) AS least_status_len_premium,
        CASE WHEN status_len >= 9 THEN 'large' WHEN status_len >= 7 THEN 'medium' ELSE 'small' END AS status_len_tier,
        CONCAT(product, '#', CAST(status_len AS STRING)) AS product_ref_2
    FROM base_policys
    WHERE end_date >= DATE '2026-02-16'
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM tagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_type = 'delay'
        )
      AND s.product_ref_2 IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT *, COUNT(*) OVER w_status AS rows_in_status
    FROM without_matches
    WINDOW w_status AS (PARTITION BY status)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    status,
    least_status_len_premium,
    status_len_tier,
    product_ref_2,
    rows_in_status
FROM metrics
WHERE holder_id >= 15
ORDER BY id DESC
