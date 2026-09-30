WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        LAST_DAY(start_date) AS start_date_month_end,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM policies
    WHERE start_date < DATE '2025-03-21'
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium_bucket, id), 3) AS premium_bucket_percent_rank,
        NTH_VALUE(start_date_month_end, 2) OVER (PARTITION BY status, product ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_month_end,
        premium - LAG(premium) OVER w_all_by_premium_bucket AS premium_change
    FROM scoped_policys
    WINDOW w_all_by_premium_bucket AS (ORDER BY premium_bucket, id)
),
unique_rows AS (
    -- Keep the earliest row per status_or_unknown
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        status_or_unknown,
        start_date_month_end,
        premium_bucket,
        premium_bucket_percent_rank,
        second_start_date_month_end,
        premium_change
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status_or_unknown ORDER BY start_date_month_end, id) AS rn
        FROM with_trends
    ) ranked
    WHERE rn = 1
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(reported_at) AS last_claim_reported_at,
        COUNT(*) AS claim_count
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        COALESCE(c.claim_count, 0) AS claim_count
    FROM unique_rows s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
with_windows AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium_bucket / NULLIF(SUM(premium_bucket) OVER (PARTITION BY claim_count), 0), 4) AS premium_bucket_share_of_claim_count,
        MIN(claim_count) OVER w_claim_count AS claim_count_min_claim_count
    FROM with_counts
    WINDOW w_claim_count AS (PARTITION BY claim_count)
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status,
        status_or_unknown,
        start_date_month_end,
        premium_bucket,
        premium_bucket_percent_rank,
        second_start_date_month_end,
        premium_change,
        last_claim_reported_at,
        claim_count,
        LAG(end_date) OVER (PARTITION BY product ORDER BY start_date ASC NULLS LAST, id) AS prev_end_date,
        SUM(claim_count) OVER w_status AS status_sum_claim_count,
        ROUND(premium_change - AVG(premium_change) OVER (PARTITION BY status), 2) AS premium_change_vs_status_avg,
        COUNT(*) OVER (PARTITION BY status) AS rows_in_status,
        ROUND(premium_bucket / NULLIF(SUM(premium_bucket) OVER w_status, 0), 4) AS premium_bucket_share_of_status
    FROM with_windows
    WINDOW w_status AS (PARTITION BY status)
)
SELECT
    id,
    holder_id,
    product,
    status,
    status_or_unknown,
    start_date_month_end,
    second_start_date_month_end,
    premium_change,
    last_claim_reported_at,
    prev_end_date,
    status_sum_claim_count,
    rows_in_status,
    premium_bucket_share_of_status,
    SUBSTRING(TRIM(status), 1, 4) AS status_prefix
FROM windowed
WHERE end_date BETWEEN DATE '2025-09-27' AND DATE '2026-11-25'
ORDER BY prev_end_date
