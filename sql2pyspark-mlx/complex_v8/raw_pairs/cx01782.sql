WITH base_claims AS (
    -- Scope the claims rows
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
        UPPER(TRIM(status)) AS status_clean
    FROM claims
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        approved_amount,
        status_clean,
        NTILE(10) OVER (PARTITION BY status ORDER BY approved_amount DESC, id) AS approved_amount_decile,
        ROUND(approved_amount - AVG(approved_amount) OVER (PARTITION BY policy_id), 2) AS approved_amount_vs_policy_id_avg,
        NTH_VALUE(claim_date, 2) OVER (PARTITION BY status ORDER BY approved_amount DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_claim_date,
        NTILE(5) OVER (PARTITION BY adjuster_id ORDER BY claim_date DESC, id) AS claim_date_quintile,
        ROUND(claimed_amount / NULLIF(SUM(claimed_amount) OVER w_policy_id, 0), 4) AS claimed_amount_share_of_policy_id
    FROM base_claims
    WINDOW w_policy_id AS (PARTITION BY policy_id)
),
qualified AS (
    -- Keep rows whose adjuster matches
    SELECT s.*
    FROM sequenced s
    WHERE EXISTS (
            SELECT 1
            FROM adjusters a
            WHERE a.id = s.adjuster_id
              AND a.team <> 'Health'
              AND UPPER(TRIM(a.adjuster_name)) LIKE 'F%'
        )
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, claim_date, claim_type, approved_amount_decile
    FROM qualified
) src
PIVOT (
    COUNT(approved_amount_decile)
    FOR claim_type IN ('medical', 'flood', 'delay')
)
