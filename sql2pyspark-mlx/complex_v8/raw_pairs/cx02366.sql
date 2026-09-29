WITH valid AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        LENGTH(TRIM(claim_type)) AS claim_type_len
    FROM claims
    WHERE (status = 'APPROVED' OR policy_id >= 13)
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_ADD(claim_date, 90) AS claim_date_plus_90d,
        YEAR(claim_date) AS claim_date_year
    FROM valid
),
latest_per_key AS (
    -- Keep the latest row per claim_type, status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        claim_type_len,
        claim_date_plus_90d,
        claim_date_year
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY claim_type, status ORDER BY claim_date_plus_90d DESC, id DESC) AS recency_rank
        FROM labeled
    ) ranked
    WHERE recency_rank = 1
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        claim_type_len,
        claim_date_plus_90d,
        COUNT(*) OVER (PARTITION BY adjuster_id) AS rows_in_adjuster_id,
        MAX(claimed_amount) OVER (PARTITION BY status ORDER BY claim_date_plus_90d ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS claimed_amount_max_7,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER (PARTITION BY adjuster_id), 0), 4) AS claimed_amount_share_of_adjuster_id,
        RANK() OVER w_status_by_claim_date_plus_90_2 AS claim_date_plus_90d_rank,
        LAG(claimed_amount) OVER (PARTITION BY status ORDER BY claim_date_plus_90d ASC, id) AS prev_claimed_amount,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY claim_date_plus_90d ASC, id), 3) AS claim_date_plus_90d_percent_rank
    FROM latest_per_key
    WINDOW w_status_by_claim_date_plus_90_2 AS (PARTITION BY status ORDER BY claim_date_plus_90d ASC)
)
-- Final projection
SELECT
    id,
    claim_type,
    status,
    claim_type_len,
    claim_date_plus_90d,
    rows_in_adjuster_id,
    claim_date_plus_90d_rank,
    prev_claimed_amount,
    claim_date_plus_90d_percent_rank
FROM windowed
ORDER BY rows_in_adjuster_id DESC, claim_date_plus_90d_percent_rank DESC NULLS FIRST
