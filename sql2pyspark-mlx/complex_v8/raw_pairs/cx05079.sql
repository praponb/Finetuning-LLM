WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT_WS(' / ', status, product) AS status_product_label
    FROM policies
    WHERE end_date < DATE '2026-04-25'
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium BETWEEN 2033.8 AND 4678.8
        )
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date
    FROM screened s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
detail_rows AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        s.status_product_label,
        c.id AS claim_id,
        c.claimed_amount AS claim_claimed_amount,
        c.status AS claim_status,
        c.claim_type
    FROM with_stats s
    LEFT JOIN claims c ON c.policy_id = s.id
)
SELECT
    id,
    holder_id,
    product,
    end_date,
    status,
    status_product_label,
    claim_id,
    claim_claimed_amount,
    claim_status,
    claim_type,
    INITCAP(TRIM(status_product_label)) AS status_product_label_clean,
    DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
FROM detail_rows
WHERE holder_id IS NOT NULL
ORDER BY product DESC, claim_id DESC, id
LIMIT 5
