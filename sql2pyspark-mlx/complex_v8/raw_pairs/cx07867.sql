WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        CONCAT(status, '#', CAST(id AS STRING)) AS status_ref,
        CONCAT_WS('|', product, status) AS product_status_label
    FROM policies
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        premium,
        end_date,
        status_ref,
        product_status_label,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium), 3) AS premium_percent_rank,
        MIN(premium) OVER w_holder_id_end_date_year AS holder_id_end_date_year_min_premium
    FROM filtered
    WINDOW w_holder_id_end_date_year AS (PARTITION BY holder_id, YEAR(end_date))
),
scored AS (
    -- Add calculated columns
    SELECT
        id,
        premium,
        end_date,
        status_ref,
        premium_percent_rank,
        DATEDIFF(DATE '2026-01-01', end_date) AS days_since_end_date
    FROM with_trends
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.premium_percent_rank,
        c.id AS claim_id,
        c.claim_date,
        c.claim_type,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id
    FROM scored s
    JOIN claims c ON c.policy_id = s.id
),
enriched_adjuster AS (
    -- Attach adjuster attributes
    SELECT
        s.id,
        s.claim_id,
        s.claim_date,
        s.claim_status,
        s.claim_adjuster_id,
        a.team AS adjuster_team,
        c.status AS claim_status_2
    FROM exploded_detail_claims s
    JOIN adjusters a ON a.id = s.claim_adjuster_id
    LEFT JOIN claims c ON c.id = s.claim_id
    WHERE a.team IS NOT NULL
),
joined AS (
    -- Join claims
    SELECT
        s.id,
        s.claim_id,
        s.claim_date,
        s.claim_status,
        s.claim_adjuster_id,
        s.claim_status_2,
        c.claimed_amount AS claim_claimed_amount,
        c.approved_amount AS claim_approved_amount
    FROM enriched_adjuster s
    JOIN claims c ON c.id = s.claim_id
)
SELECT id, claim_id, field, value
FROM (
    SELECT id, claim_id, claim_claimed_amount, claim_approved_amount
    FROM joined
) src
UNPIVOT INCLUDE NULLS (
    value FOR field IN (claim_claimed_amount, claim_approved_amount)
) unp
