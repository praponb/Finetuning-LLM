WITH base AS (
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
        DATE_TRUNC('QUARTER', claim_date) AS claim_date_quarter
    FROM claims
    WHERE HOUR(reported_at) BETWEEN 7 AND 11
),
annotated AS (
    -- Add calculated columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        claim_date_quarter,
        DATE_TRUNC('WEEK', claim_date) AS claim_date_week,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM base
    WHERE claim_type IS NOT NULL
),
ranked AS (
    -- Running and moving aggregates
    SELECT
        id,
        policy_id,
        claimed_amount,
        approved_amount,
        status,
        claim_date_quarter,
        claim_date_week,
        FIRST_VALUE(claim_date_week) OVER (PARTITION BY claim_type, policy_id ORDER BY claimed_amount ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_claim_date_week,
        FIRST_VALUE(approved_amount) IGNORE NULLS OVER (ORDER BY claim_date_week, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_approved_amount,
        ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY claim_date_quarter ASC, id) AS claim_date_quarter_seq,
        ROUND(approved_amount / NULLIF(SUM(approved_amount) OVER (PARTITION BY claim_type), 0), 4) AS approved_amount_share_of_claim_type,
        SUM(claimed_amount) OVER (PARTITION BY claim_type) AS claim_type_sum_claimed_amount,
        ROUND((UNIX_TIMESTAMP(claim_date_quarter) - UNIX_TIMESTAMP(LAG(claim_date_quarter) OVER (ORDER BY claim_date_week, id))) / 60, 1) AS minutes_since_prev_claim_date_quarter
    FROM annotated
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        claim_date_quarter_seq - LAG(claim_date_quarter_seq) OVER w_claim_date_quarter_seq_by_ap AS claim_date_quarter_seq_change,
        SUM(first_known_approved_amount) OVER (ORDER BY claim_date_quarter ASC NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_first_known_approved_amount,
        MAX(claim_date_quarter_seq) OVER (PARTITION BY policy_id, status) AS policy_id_status_max_claim_date_quarter_seq,
        SUM(first_known_approved_amount) OVER (PARTITION BY policy_id, status) AS policy_id_status_sum_first_known_approved_amount
    FROM ranked
    WINDOW w_claim_date_quarter_seq_by_ap AS (PARTITION BY claim_date_quarter_seq ORDER BY approved_amount_share_of_claim_type DESC, id)
)
-- Subtotals with ROLLUP
SELECT
    COALESCE(status, 'ALL') AS status,
    claim_date_quarter_seq,
    COUNT(DISTINCT status) AS distinct_status,
    COUNT(*) AS n_rows,
    SUM(CASE WHEN status IS NOT NULL THEN minutes_since_prev_claim_date_quarter ELSE 0 END) AS minutes_since_prev_claim_date_quarter_status_is_
FROM windowed
GROUP BY ROLLUP(status, claim_date_quarter_seq)
