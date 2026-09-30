WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM claims
    WHERE approved_amount <= 23120.7
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        id,
        policy_id,
        adjuster_id,
        status,
        reported_at,
        LEAD(claim_date) OVER (ORDER BY claim_date DESC NULLS FIRST, id) AS next_claim_date,
        ROW_NUMBER() OVER w_claim_type_status_by_reporte AS rn,
        LEAD(reported_at) OVER (PARTITION BY claim_type, status ORDER BY reported_at, id) AS next_reported_at,
        NTH_VALUE(reported_at, 2) OVER (PARTITION BY status ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_reported_at,
        ROUND(CUME_DIST() OVER (PARTITION BY adjuster_id, status ORDER BY reported_at ASC NULLS LAST, id), 3) AS reported_at_cume_dist,
        NTILE(3) OVER (PARTITION BY adjuster_id, status ORDER BY reported_at ASC NULLS LAST, id) AS reported_at_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY reported_at), 3) AS reported_at_percent_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_type, status ORDER BY reported_at), 3) AS reported_at_percent_rank_2,
        LAG(claim_date, 2) OVER w_status_by_reported_at AS prev2_claim_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY MONTH(claim_date) ORDER BY claim_date NULLS LAST), 3) AS claim_date_percent_rank,
        LAST_VALUE(reported_at) OVER (ORDER BY claim_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_reported_at,
        LEAD(reported_at, 3) OVER (ORDER BY claim_date DESC NULLS FIRST, id) AS next3_reported_at,
        ROUND(CUME_DIST() OVER w_all_by_claim_date_ties, 3) AS claim_date_cume_dist,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY claim_type, status ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        ROUND(PERCENT_RANK() OVER w_all_by_claim_date_ties, 3) AS claim_date_percent_rank_2
    FROM cleaned_claims
    WINDOW w_claim_type_status_by_reporte AS (PARTITION BY claim_type, status ORDER BY reported_at, id), w_status_by_reported_at AS (PARTITION BY status ORDER BY reported_at, id), w_all_by_claim_date_ties AS (ORDER BY claim_date DESC)
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    status,
    next_claim_date,
    rn,
    second_reported_at,
    reported_at_cume_dist,
    reported_at_percent_rank_2,
    prev2_claim_date,
    claim_date_percent_rank,
    next3_reported_at,
    claim_date_cume_dist,
    second_claim_date
FROM with_windows
WHERE adjuster_id IS NOT NULL
