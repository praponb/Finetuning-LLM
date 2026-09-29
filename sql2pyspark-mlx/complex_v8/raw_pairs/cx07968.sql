WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE start_date >= DATE '2024-12-02'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MIN(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS min_claim_approved_amount
    FROM scoped s
),
labeled AS (
    -- Derive helper columns
    SELECT *, DATE_TRUNC('MONTH', end_date) AS end_date_month
    FROM with_stats
),
joined_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.end_date,
        s.status,
        s.product_ref,
        s.days_start_date_to_end_date,
        s.min_claim_approved_amount,
        s.end_date_month,
        p.holder_name
    FROM labeled s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT *
FROM (
    SELECT end_date, product, min_claim_approved_amount
    FROM joined_holder
) src
PIVOT (
    MIN(min_claim_approved_amount)
    FOR product IN ('AUTO', 'HOME', 'HEALTH', 'LIFE')
)
ORDER BY end_date
