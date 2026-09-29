WITH prepared AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len
    FROM policies
    WHERE status = 'CANCELLED'
),
claim_stats AS (
    -- Per-policy claims metrics
    SELECT
        policy_id,
        MIN(claim_date) AS first_claim_date,
        MAX(claim_date) AS last_claim_date
    FROM claims
    GROUP BY policy_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.first_claim_date,
        c.last_claim_date
    FROM prepared s
    LEFT JOIN claim_stats c ON c.policy_id = s.id
)
SELECT *
FROM (
    SELECT holder_id, product, status_len
    FROM profiled
) src
PIVOT (
    AVG(status_len) AS avg_v, MAX(status_len) AS max_v
    FOR product IN ('TRAVEL', 'HOME', 'LIFE', 'HEALTH')
)
ORDER BY holder_id
