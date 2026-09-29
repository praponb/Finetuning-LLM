WITH base_claims AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        approved_amount,
        status,
        reported_at,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM claims
    WHERE reported_at >= TIMESTAMP '2025-01-19 14:00:00'
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(approved_amount) OVER (PARTITION BY MONTH(claim_date), policy_id) AS claim_date_month_policy_id_max_approved_amount,
        RANK() OVER (PARTITION BY status ORDER BY approved_amount DESC) AS approved_amount_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status ORDER BY approved_amount DESC, id), 3) AS approved_amount_percent_rank,
        NTH_VALUE(approved_amount, 2) OVER (PARTITION BY status ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_approved_amount
    FROM base_claims
    WINDOW w_status_by_approved_amount_ti AS (PARTITION BY status ORDER BY approved_amount DESC)
),
enriched AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT_WS('-', status, status_prefix) AS status_status_prefix_label,
        FLOOR(approved_amount / 10000) * 10000 AS approved_amount_bucket
    FROM ranked
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM enriched s
    WHERE s.adjuster_id IN (
            SELECT c.adjuster_id
            FROM claims c
        )
      AND s.status = 'IN_REVIEW'
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        COALESCE(approved_amount_rank, 0) AS approved_amount_rank_filled,
        LOWER(REGEXP_REPLACE(TRIM(status_prefix), ' ', '_')) AS status_prefix_slug,
        GREATEST(second_approved_amount, approved_amount_bucket) AS greatest_second_approved_amount_approved_amount_
    FROM filtered
)
-- Final projection
SELECT
    id,
    policy_id,
    adjuster_id,
    claim_date,
    approved_amount,
    reported_at,
    status_prefix,
    claim_date_month_policy_id_max_approved_amount,
    approved_amount_rank,
    approved_amount_percent_rank,
    second_approved_amount,
    status_status_prefix_label,
    approved_amount_bucket,
    approved_amount_rank_filled,
    status_prefix_slug,
    greatest_second_approved_amount_approved_amount_,
    UPPER(TRIM(status)) AS status_clean
FROM annotated
ORDER BY claim_date, approved_amount_rank
