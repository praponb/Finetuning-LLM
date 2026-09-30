WITH cleaned AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        reported_at,
        reported_at + INTERVAL 7 DAYS AS reported_at_plus_7d,
        QUARTER(reported_at) AS reported_at_quarter
    FROM claims
    WHERE policy_id >= 16
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY reported_at DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY MONTH(claim_date) ORDER BY approved_amount, id) AS rn,
        SUM(claimed_amount) OVER (PARTITION BY policy_id) AS policy_id_sum_claimed_amount,
        NTILE(3) OVER (PARTITION BY reported_at_quarter ORDER BY reported_at_quarter ASC, id) AS reported_at_quarter_tercile,
        LAG(reported_at) OVER (PARTITION BY reported_at_quarter, DATE_TRUNC('MONTH', claim_date) ORDER BY reported_at_plus_7d, id) AS prev_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY reported_at_quarter ORDER BY reported_at_quarter ASC, id), 3) AS reported_at_quarter_percent_rank,
        ROUND(reported_at_quarter / NULLIF(SUM(reported_at_quarter) OVER (PARTITION BY policy_id), 0), 4) AS reported_at_quarter_share_of_policy_id,
        ROUND(AVG(approved_amount) OVER (PARTITION BY policy_id), 2) AS policy_id_avg_approved_amount,
        ROUND(CUME_DIST() OVER (PARTITION BY reported_at_quarter, adjuster_id ORDER BY reported_at_quarter), 3) AS reported_at_quarter_cume_dist,
        LAG(claimed_amount) OVER w_reported_at_quarter_claim_da AS prev_claimed_amount
    FROM cleaned
    WINDOW w_reported_at_quarter_claim_da AS (PARTITION BY reported_at_quarter, DATE_TRUNC('MONTH', claim_date) ORDER BY reported_at_plus_7d, id)
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    reported_at_plus_7d,
    reported_at_quarter,
    last_known_approved_amount,
    rn,
    reported_at_quarter_tercile,
    reported_at_quarter_percent_rank,
    policy_id_avg_approved_amount,
    reported_at_quarter_cume_dist,
    prev_claimed_amount,
    approved_amount - claimed_amount AS approved_amount_minus_claimed_amount
FROM windowed
ORDER BY claim_date DESC NULLS FIRST, approved_amount, id
LIMIT 20
