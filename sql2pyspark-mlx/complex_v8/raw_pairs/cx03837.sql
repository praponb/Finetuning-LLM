WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date
    FROM policies
),
combined_details_holder AS (
    -- Enrich with dimension data
    SELECT s.*, p.joined_on AS holder_joined_on
    FROM valid_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
with_details_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.end_date,
        p.segment AS holder_segment,
        p.region AS holder_region
    FROM combined_details_holder s
    JOIN policyholders p ON p.id = s.holder_id
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_details_holder s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
            WHERE c.claim_date BETWEEN DATE '2024-04-11' AND DATE '2025-11-15'
        )
)
SELECT *
FROM (
    SELECT end_date, start_date, holder_segment, premium
    FROM without_matches
) src
PIVOT (
    SUM(premium)
    FOR holder_segment IN ('SME', 'RETAIL', 'VIP')
)
