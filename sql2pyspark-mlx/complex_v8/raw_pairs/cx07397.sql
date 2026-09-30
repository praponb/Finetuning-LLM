WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        UPPER(TRIM(status)) AS status_clean
    FROM policies
    WHERE (start_date < DATE '2025-05-30' OR holder_id <= 6)
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.premium_adj,
        s.status_clean,
        p.segment AS holder_segment,
        p.region AS holder_region,
        p.risk_score AS holder_risk_score
    FROM cleaned_policys s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium, id), 3) AS premium_percent_rank,
        SUM(holder_risk_score) OVER w_status_holder_id AS status_holder_id_sum_holder_risk_score,
        ROUND(AVG(premium) OVER (ORDER BY premium, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_1_prior,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium), 3) AS premium_percent_rank_2,
        FIRST_VALUE(premium_adj) IGNORE NULLS OVER (ORDER BY premium, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_adj
    FROM with_details_holder
    WINDOW w_status_holder_id AS (PARTITION BY status, holder_id)
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT MAX(c.claimed_amount) FROM claims c WHERE c.policy_id = s.id) AS max_claim_claimed_amount
    FROM windowed s
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    status,
    premium_adj,
    status_clean,
    holder_segment,
    holder_region,
    holder_risk_score,
    premium_percent_rank,
    status_holder_id_sum_holder_risk_score,
    last_known_start_date,
    first_known_premium_adj,
    max_claim_claimed_amount,
    ADD_MONTHS(start_date, 6) AS start_date_plus_6m
FROM profiled
WHERE last_known_start_date BETWEEN DATE '2024-06-19' AND DATE '2025-07-07'
ORDER BY premium NULLS LAST, premium_adj
