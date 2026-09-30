WITH prepared AS (
    -- Filter claims
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        COALESCE(claimed_amount, 0.0) AS claimed_amount_filled
    FROM claims
    WHERE claimed_amount <= 32441.2
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        claimed_amount,
        status,
        reported_at,
        claimed_amount_filled,
        LOWER(TRIM(status)) AS status_clean
    FROM prepared
    WHERE claimed_amount BETWEEN 12726.7 AND 41878.5
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.status = 'OPEN'
        )
),
filtered_2 AS (
    -- Keep rows whose policy matches
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.id = s.policy_id
              AND p.status = 'LAPSED'
              AND p.product = 'HOME'
        )
),
joined_policy AS (
    -- Attach policy attributes
    SELECT
        s.id,
        s.policy_id,
        s.adjuster_id,
        s.claimed_amount,
        s.reported_at,
        s.claimed_amount_filled,
        s.status_clean,
        p.holder_id AS policy_holder_id,
        p.start_date AS policy_start_date,
        p.end_date AS policy_end_date
    FROM filtered_2 s
    LEFT JOIN policies p ON p.id = s.policy_id
),
unique_rows AS (
    -- Keep the latest row per adjuster_id, policy_id
    SELECT
        id,
        policy_id,
        adjuster_id,
        claimed_amount,
        reported_at,
        claimed_amount_filled,
        status_clean,
        policy_holder_id,
        policy_start_date,
        policy_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY adjuster_id, policy_id ORDER BY claimed_amount DESC, id DESC) AS row_num
        FROM joined_policy
    ) ranked
    WHERE row_num = 1
),
grouped AS (
    -- Group-level totals
    SELECT
        policy_holder_id,
        adjuster_id,
        PERCENTILE_APPROX(claimed_amount_filled, 0.5) AS median_claimed_amount_filled
    FROM unique_rows
    GROUP BY policy_holder_id, adjuster_id
    HAVING MAX(claimed_amount) > 10518.4
)
-- Final projection
SELECT policy_holder_id
FROM grouped
ORDER BY policy_holder_id DESC NULLS FIRST
