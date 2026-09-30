WITH base AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix
    FROM policies
    WHERE premium > 2111.3
      AND end_date BETWEEN DATE '2025-11-20' AND DATE '2026-10-23'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MAX(reported_at) AS last_claim_reported_at,
        MAX(claimed_amount) AS max_claim_claimed_amount
    FROM claims
    GROUP BY policy_id
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_reported_at,
        c.max_claim_claimed_amount
    FROM base s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        ROUND(premium / NULLIF(max_claim_claimed_amount, 0), 3) AS premium_per_max_claim_claimed_amount
    FROM with_counts
    WHERE premium > 1132.1
),
combined_details AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.status,
        s.days_end_date_to_start_date,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM annotated s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
grouped AS (
    -- Summarise per group
    SELECT
        holder_id,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT holder_segment) AS distinct_holder_segments,
        COUNT(DISTINCT id) AS distinct_ids
    FROM combined_details
    WHERE holder_joined_on >= DATE '2024-07-18'
    GROUP BY 1
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        holder_id,
        n_rows,
        distinct_holder_segments,
        distinct_ids,
        MIN(n_rows) OVER (PARTITION BY distinct_ids, n_rows) AS distinct_ids_n_rows_min_n_rows,
        ROUND(distinct_holder_segments - AVG(distinct_holder_segments) OVER (PARTITION BY distinct_ids, n_rows), 2) AS distinct_holder_segments_vs_distinct_ids_n_rows_,
        LAST_VALUE(distinct_holder_segments) OVER (PARTITION BY distinct_holder_segments ORDER BY n_rows DESC, holder_id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_distinct_holder_segments,
        ROUND(n_rows - AVG(n_rows) OVER (PARTITION BY distinct_ids, n_rows), 2) AS n_rows_vs_distinct_ids_n_rows_avg,
        ROUND(PERCENT_RANK() OVER (PARTITION BY distinct_holder_segments ORDER BY distinct_ids NULLS LAST), 3) AS distinct_ids_percent_rank,
        distinct_holder_segments - LAG(distinct_holder_segments) OVER (PARTITION BY distinct_holder_segments ORDER BY n_rows DESC, holder_id) AS distinct_holder_segments_change
    FROM grouped
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM metrics
    WHERE distinct_ids > (SELECT MIN(distinct_ids) FROM metrics)
)
SELECT holder_id, field, reading
FROM (
    SELECT holder_id, n_rows_vs_distinct_ids_n_rows_avg, distinct_ids_percent_rank
    FROM with_activity
) src
UNPIVOT INCLUDE NULLS (
    reading FOR field IN (n_rows_vs_distinct_ids_n_rows_avg, distinct_ids_percent_rank)
) unp
