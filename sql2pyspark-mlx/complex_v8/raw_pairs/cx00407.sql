WITH cleaned_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        ROUND(claimed_amount * 0.9, 2) AS claimed_amount_adj,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE adjuster_id >= 7
      AND claim_type IS NOT NULL
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        claimed_amount_adj,
        MAX(approved_amount) OVER (PARTITION BY status) AS status_max_approved_amount,
        LAG(claim_date) OVER (PARTITION BY adjuster_id, status ORDER BY approved_amount ASC NULLS LAST, id) AS prev_claim_date,
        LEAD(reported_at) OVER (PARTITION BY adjuster_id, status ORDER BY approved_amount ASC NULLS LAST, id) AS next_reported_at
    FROM cleaned_claims
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        adjuster_id,
        approved_amount,
        status,
        reported_at,
        status_max_approved_amount,
        prev_claim_date,
        next_reported_at,
        FLOOR(claimed_amount_adj / 10000) * 10000 AS claimed_amount_adj_bucket
    FROM windowed
    WHERE reported_at >= TIMESTAMP '2025-01-19 14:00:00'
),
screened AS (
    -- Compare against the overall avg
    SELECT *
    FROM derived
    WHERE approved_amount > (SELECT AVG(approved_amount) FROM derived)
),
with_trends AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        LAST_VALUE(reported_at) IGNORE NULLS OVER (PARTITION BY adjuster_id, status ORDER BY status_max_approved_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_reported_at,
        ROUND(claimed_amount_adj_bucket / NULLIF(SUM(claimed_amount_adj_bucket) OVER (PARTITION BY status, adjuster_id), 0), 4) AS claimed_amount_adj_bucket_share_of_status_adjust,
        ROUND(status_max_approved_amount - AVG(status_max_approved_amount) OVER (PARTITION BY status, adjuster_id), 2) AS status_max_approved_amount_vs_status_adjuster_id,
        NTILE(3) OVER w_all_by_approved_amount AS approved_amount_tercile
    FROM screened
    WINDOW w_all_by_approved_amount AS (ORDER BY approved_amount DESC NULLS FIRST, id)
),
per_group AS (
    -- Aggregate with conditional metrics
    SELECT
        YEAR(reported_at) AS reported_at_year,
        COUNT(*) FILTER (WHERE status = 'IN_REVIEW') AS n_status_in_review,
        MAX(claimed_amount_adj_bucket) AS max_claimed_amount_adj_bucket,
        PERCENTILE_APPROX(status_max_approved_amount_vs_status_adjuster_id, 0.5) AS median_status_max_approved_amount_vs_status_adju
    FROM with_trends
    GROUP BY YEAR(reported_at)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM per_group
    WHERE max_claimed_amount_adj_bucket > (SELECT AVG(max_claimed_amount_adj_bucket) FROM per_group)
)
SELECT
    reported_at_year,
    n_status_in_review,
    max_claimed_amount_adj_bucket,
    median_status_max_approved_amount_vs_status_adju,
    CASE WHEN n_status_in_review >= 8 THEN 'A' WHEN n_status_in_review >= 4 THEN 'B' ELSE 'C' END AS n_status_in_review_tier
FROM without_matches
ORDER BY n_status_in_review
