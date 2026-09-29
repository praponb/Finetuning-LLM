WITH base AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        DATEDIFF(DATE '2026-01-01', reported_at) AS days_since_reported_at,
        CONCAT_WS(': ', claim_type, status) AS claim_type_status_label
    FROM claims
    WHERE adjuster_id IS NOT NULL
),
summary AS (
    -- Group-level totals
    SELECT
        YEAR(claim_date) AS claim_date_year,
        MIN(days_since_reported_at) AS min_days_since_reported_at,
        ROUND(STDDEV(days_since_reported_at), 2) AS stddev_days_since_reported_at,
        COUNT(*) AS row_count,
        ROUND(100 * COUNT(CASE WHEN status = 'REJECTED' THEN 1 END) / COUNT(*), 1) AS pct_status_rejected
    FROM base
    WHERE adjuster_id IS NOT NULL
    GROUP BY 1
),
derived AS (
    -- Add calculated columns
    SELECT
        claim_date_year,
        row_count,
        GREATEST(row_count, pct_status_rejected) AS greatest_row_count_pct_status_rejected,
        COALESCE(row_count, 0) AS row_count_filled,
        row_count - claim_date_year AS row_count_minus_claim_date_year
    FROM summary
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(claim_date_year) OVER w_row_count AS row_count_sum_claim_date_year,
        RANK() OVER (ORDER BY row_count NULLS LAST) AS row_count_rank,
        MIN(claim_date_year) OVER (PARTITION BY row_count) AS row_count_min_claim_date_year
    FROM derived
    WINDOW w_row_count AS (PARTITION BY row_count)
),
annotated AS (
    -- Label and score rows
    SELECT
        claim_date_year,
        row_count,
        greatest_row_count_pct_status_rejected,
        row_count_filled,
        row_count_minus_claim_date_year,
        row_count_sum_claim_date_year,
        row_count_rank,
        row_count_min_claim_date_year,
        CASE WHEN row_count_filled >= 9 THEN 'A' WHEN row_count_filled >= 4 THEN 'B' ELSE 'C' END AS row_count_filled_tier,
        FLOOR(row_count_rank / 2) * 2 AS row_count_rank_bucket,
        LEAST(row_count_rank, row_count_filled) AS least_row_count_rank_row_count_filled
    FROM windowed
    WHERE greatest_row_count_pct_status_rejected < 75.0
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        claim_date_year,
        row_count,
        greatest_row_count_pct_status_rejected,
        row_count_filled,
        row_count_rank_bucket,
        least_row_count_rank_row_count_filled,
        MIN(row_count_rank) OVER (PARTITION BY claim_date_year) AS claim_date_year_min_row_count_rank,
        SIZE(COLLECT_SET(row_count_filled_tier) OVER (PARTITION BY claim_date_year)) AS distinct_row_count_filled_tier_in_claim_date_yea
    FROM annotated
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        row_count - row_count_rank_bucket AS row_count_minus_row_count_rank_bucket
    FROM with_windows
    WHERE row_count_rank_bucket > 3
)
SELECT
    claim_date_year,
    row_count,
    row_count_filled,
    row_count_rank_bucket,
    least_row_count_rank_row_count_filled,
    distinct_row_count_filled_tier_in_claim_date_yea,
    row_count_minus_row_count_rank_bucket,
    ABS(row_count_filled - 8) AS row_count_filled_distance,
    COALESCE(row_count_rank_bucket, 0) AS row_count_rank_bucket_filled
FROM enriched
ORDER BY claim_date_year, row_count DESC
LIMIT 20
