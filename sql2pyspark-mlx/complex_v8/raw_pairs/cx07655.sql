WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status
    FROM claims
    WHERE reported_at < TIMESTAMP '2025-02-24 13:00:00'
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        adjuster_id,
        approved_amount,
        status,
        ROUND(approved_amount - AVG(approved_amount) OVER w_status, 2) AS approved_amount_vs_status_avg,
        LAST_VALUE(claim_date) OVER (ORDER BY claim_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_claim_date
    FROM base
    WINDOW w_status AS (PARTITION BY status)
),
claim_stats AS (
    -- Per-adjuster claims metrics
    SELECT adjuster_id, MAX(claim_date) AS last_claim_date_2
    FROM claims
    GROUP BY adjuster_id
),
profiled AS (
    -- Attach claims metrics
    SELECT
        s.*,
        c.last_claim_date_2
    FROM windowed s
    LEFT JOIN claim_stats c ON c.adjuster_id = s.adjuster_id
),
enriched AS (
    -- Derive helper columns
    SELECT *, COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM profiled
)
SELECT DISTINCT status, status_or_unknown, last_claim_date
FROM enriched
ORDER BY last_claim_date NULLS LAST, status_or_unknown
