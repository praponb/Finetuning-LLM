WITH scoped AS (
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
        TO_DATE(reported_at) AS reported_at_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown
    FROM claims
    WHERE approved_amount < 22250.6
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scoped s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.adjuster_id = s.adjuster_id
              AND c.claimed_amount < 32796.4
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        MAX(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS approved_amount_max_2_prior,
        MIN(approved_amount) OVER (ORDER BY reported_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_approved_amount_so_far,
        ROUND(CUME_DIST() OVER (ORDER BY reported_at), 3) AS reported_at_cume_dist
    FROM filtered
)
SELECT
    id,
    adjuster_id,
    claim_date,
    claim_type,
    claimed_amount,
    approved_amount,
    status,
    reported_at,
    reported_at_date,
    status_slug,
    status_or_unknown,
    approved_amount_max_2_prior,
    min_approved_amount_so_far,
    reported_at_cume_dist
FROM metrics
WHERE status IN ('APPROVED', 'REJECTED', 'PAID')
ORDER BY approved_amount DESC
