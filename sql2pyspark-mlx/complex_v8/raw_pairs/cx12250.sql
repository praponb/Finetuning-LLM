WITH scoped AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        ADD_MONTHS(claim_date, 12) AS claim_date_plus_12m,
        CASE WHEN claim_type IN ('collision', 'theft') THEN 'group_a' WHEN claim_type = 'medical' THEN 'group_b' ELSE 'other' END AS claim_type_group
    FROM claims
    WHERE claim_date IS NOT NULL
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(AVG(claimed_amount) OVER (ORDER BY claim_date_plus_12m DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS claimed_amount_avg_7,
        LAG(claim_date_plus_12m) OVER (ORDER BY claim_date_plus_12m DESC, id) AS prev_claim_date_plus_12m,
        DATEDIFF(claim_date_plus_12m, LAG(claim_date_plus_12m) OVER (ORDER BY claim_date, id)) AS days_since_prev_claim_date_plus_12m,
        ROW_NUMBER() OVER (ORDER BY claim_date_plus_12m DESC, id) AS seq_no
    FROM scoped
),
filtered AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.status IS NOT NULL
        )
),
joined AS (
    -- Join policies
    SELECT
        s.*,
        p.status AS policy_status,
        p.holder_id AS policy_holder_id,
        p.end_date AS policy_end_date
    FROM filtered s
    JOIN policies p ON p.id = s.policy_id
),
first_rows AS (
    -- Top 3 per group
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        status,
        claim_date_plus_12m,
        claim_type_group,
        claimed_amount_avg_7,
        prev_claim_date_plus_12m,
        days_since_prev_claim_date_plus_12m,
        policy_status,
        policy_holder_id,
        policy_end_date
    FROM joined
    WHERE seq_no <= 3
),
with_children AS (
    -- One row per policy
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claim_type,
        s.status,
        s.claim_date_plus_12m,
        s.days_since_prev_claim_date_plus_12m,
        s.policy_holder_id,
        p.id AS policy_id_2,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date_2,
        p.product AS policy_product
    FROM first_rows s
    LEFT JOIN policies p ON p.holder_id = s.policy_holder_id AND p.premium >= 1042.5
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_count,
        (SELECT COUNT(CASE WHEN c.status <> 'REJECTED' THEN 1 END) FROM claims c WHERE c.policy_id = s.policy_id) AS claim_status_count
    FROM with_children s
)
-- Final projection
SELECT
    id,
    policy_id,
    claim_type,
    status,
    claim_date_plus_12m,
    policy_holder_id,
    policy_id_2,
    policy_end_date_2,
    policy_product,
    claim_count,
    claim_status_count
FROM profiled
ORDER BY claim_status_count
