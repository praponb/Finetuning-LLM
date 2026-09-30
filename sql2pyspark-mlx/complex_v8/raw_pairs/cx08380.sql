WITH cleaned AS (
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
        MONTH(reported_at) AS reported_at_month,
        TO_DATE(reported_at) AS reported_at_date,
        FLOOR(claimed_amount / 10000) * 10000 AS claimed_amount_bucket
    FROM claims
    WHERE approved_amount <= 23120.7
),
combined_details AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_date,
        s.claim_type,
        s.approved_amount,
        s.status,
        s.reported_at,
        s.reported_at_month,
        s.reported_at_date,
        s.claimed_amount_bucket,
        p.holder_id AS policy_holder_id
    FROM cleaned s
    JOIN policies p ON p.id = s.policy_id
),
unique_rows AS (
    -- Keep the earliest row per status
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status,
        reported_at,
        reported_at_month,
        reported_at_date,
        claimed_amount_bucket,
        policy_holder_id
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY reported_at, id) AS row_num
        FROM combined_details
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(reported_at) OVER w_reported_at_month_policy_id AS next_reported_at,
        RANK() OVER (PARTITION BY reported_at_month, policy_id ORDER BY reported_at_month NULLS LAST) AS reported_at_month_rank,
        DENSE_RANK() OVER w_policy_id_adjuster_id_by_rep AS reported_at_date_dense_rank,
        ROUND(claimed_amount_bucket - AVG(claimed_amount_bucket) OVER (PARTITION BY adjuster_id), 2) AS claimed_amount_bucket_vs_adjuster_id_avg
    FROM unique_rows
    WINDOW w_reported_at_month_policy_id AS (PARTITION BY reported_at_month, policy_id ORDER BY reported_at_month NULLS LAST, id), w_policy_id_adjuster_id_by_rep AS (PARTITION BY policy_id, adjuster_id ORDER BY reported_at_date DESC, id)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        adjuster_id,
        policy_id,
        MAX(reported_at_date) AS last_reported_at_date,
        PERCENTILE_APPROX(reported_at_month, 0.5) AS median_reported_at_month,
        COUNT(*) AS cnt,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list
    FROM metrics
    GROUP BY adjuster_id, policy_id
    HAVING COUNT(*) >= 3
),
without_matches AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM summary s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.approved_amount BETWEEN 0.0 AND 23487.5
        )
)
-- Final projection
SELECT
    adjuster_id,
    policy_id,
    last_reported_at_date,
    median_reported_at_month,
    DATE_TRUNC('MONTH', last_reported_at_date) AS last_reported_at_date_month
FROM without_matches
WHERE cnt < 8
ORDER BY median_reported_at_month DESC, last_reported_at_date_month
