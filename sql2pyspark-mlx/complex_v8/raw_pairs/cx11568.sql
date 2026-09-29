WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE end_date BETWEEN DATE '2025-12-05' AND DATE '2026-09-19'
      AND holder_id <= 4
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.end_date,
        s.status,
        p.holder_name,
        p.joined_on AS holder_joined_on
    FROM cleaned_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
deduped AS (
    -- Keep the latest row per holder_id, product
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        holder_name,
        holder_joined_on
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id, product ORDER BY holder_joined_on DESC, id DESC) AS row_num
        FROM combined_details_holder
    ) ranked
    WHERE row_num = 1
),
with_details_holder AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.end_date,
        s.holder_name,
        s.holder_joined_on,
        p.holder_name AS holder_holder_name,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on_2
    FROM deduped s
    JOIN policyholders p ON p.id = s.holder_id
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT policy_id, MAX(approved_amount) AS max_claim_approved_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.max_claim_approved_amount
    FROM with_details_holder s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
holder_id_stats AS (
    -- Avg max_claim_approved_amount per holder_id
    SELECT holder_id, AVG(max_claim_approved_amount) AS avg_max_claim_approved_amount
    FROM with_counts
    GROUP BY holder_id
),
above_average AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_max_claim_approved_amount,
        ROUND(s.max_claim_approved_amount - g.avg_max_claim_approved_amount, 2) AS max_claim_approved_amount_minus_avg
    FROM with_counts s
    JOIN holder_id_stats g ON g.holder_id = s.holder_id
    WHERE s.max_claim_approved_amount > g.avg_max_claim_approved_amount
)
SELECT
    id,
    holder_id,
    end_date,
    holder_joined_on,
    holder_holder_name,
    holder_joined_on_2,
    max_claim_approved_amount,
    avg_max_claim_approved_amount,
    max_claim_approved_amount_minus_avg,
    DATE_FORMAT(holder_joined_on, 'yyyy-MM-dd') AS holder_joined_on_label,
    CONCAT_WS(': ', holder_holder_name, holder_name) AS holder_holder_name_holder_name_label
FROM above_average
WHERE holder_joined_on >= DATE '2024-10-23'
ORDER BY holder_joined_on_label DESC, holder_holder_name DESC, id
LIMIT 10
