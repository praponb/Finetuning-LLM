WITH recent AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        status,
        reported_at
    FROM claims
    WHERE status NOT IN ('PAID', 'APPROVED')
),
windowed AS (
    -- Running and moving aggregates
    SELECT
        id,
        adjuster_id,
        status,
        reported_at,
        ROUND((UNIX_TIMESTAMP(reported_at) - UNIX_TIMESTAMP(LAG(reported_at) OVER (PARTITION BY TO_DATE(reported_at) ORDER BY claim_date, id))) / 60, 1) AS minutes_since_prev_reported_at,
        ROUND(PERCENT_RANK() OVER (PARTITION BY adjuster_id ORDER BY claim_date), 3) AS claim_date_percent_rank,
        RANK() OVER (PARTITION BY adjuster_id ORDER BY claim_date) AS claim_date_rank,
        RANK() OVER (PARTITION BY TO_DATE(reported_at) ORDER BY claim_date) AS claim_date_rank_2,
        DATEDIFF(claim_date, LAG(claim_date) OVER w_adjuster_id_by_claim_date) AS days_since_prev_claim_date,
        ROUND(PERCENT_RANK() OVER w_policy_id_by_claim_date, 3) AS claim_date_percent_rank_2,
        LEAD(claim_date) OVER (PARTITION BY status ORDER BY claim_date ASC, id) AS next_claim_date,
        NTILE(10) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id) AS claim_date_decile,
        FIRST_VALUE(reported_at) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_reported_at,
        ROW_NUMBER() OVER w_adjuster_id_by_claim_date AS rn,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        NTILE(3) OVER (PARTITION BY adjuster_id ORDER BY claim_date, id) AS claim_date_tercile
    FROM recent
    WINDOW w_adjuster_id_by_claim_date AS (PARTITION BY adjuster_id ORDER BY claim_date, id), w_policy_id_by_claim_date AS (PARTITION BY policy_id ORDER BY claim_date DESC, id)
),
annotated AS (
    -- Add calculated columns
    SELECT
        *,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        QUARTER(second_claim_date) AS second_claim_date_quarter
    FROM windowed
    WHERE days_since_prev_claim_date >= 30
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        status,
        reported_at,
        minutes_since_prev_reported_at,
        claim_date_percent_rank,
        claim_date_rank,
        claim_date_rank_2,
        days_since_prev_claim_date,
        first_reported_at,
        second_claim_date,
        claim_date_tercile,
        second_claim_date_quarter,
        ROUND(AVG(claim_date_percent_rank_2) OVER w_rn, 2) AS rn_avg_claim_date_percent_rank_2,
        SUM(second_claim_date_quarter) OVER w_rn AS rn_sum_second_claim_date_quarter,
        RANK() OVER (ORDER BY minutes_since_prev_reported_at) AS minutes_since_prev_reported_at_rank,
        COUNT(*) OVER w_rn AS rows_in_rn
    FROM annotated
    WINDOW w_rn AS (PARTITION BY rn)
)
-- Final projection
SELECT
    id,
    status,
    reported_at,
    minutes_since_prev_reported_at,
    claim_date_percent_rank,
    claim_date_rank,
    claim_date_rank_2,
    first_reported_at,
    second_claim_date,
    claim_date_tercile,
    second_claim_date_quarter,
    rn_sum_second_claim_date_quarter,
    minutes_since_prev_reported_at_rank,
    SUBSTRING(TRIM(status), 1, 2) AS status_prefix
FROM sequenced
ORDER BY claim_date_percent_rank DESC, second_claim_date DESC
