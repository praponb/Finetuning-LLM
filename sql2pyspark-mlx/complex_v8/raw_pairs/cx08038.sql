WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE holder_id IS NOT NULL
      AND premium <= 3719.7
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        NTILE(4) OVER (ORDER BY premium DESC, id) AS premium_quartile,
        ROUND(premium - AVG(premium) OVER (PARTITION BY YEAR(start_date), holder_id), 2) AS premium_vs_start_date_year_holder_id_avg
    FROM scoped_policys
),
line_level AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        c.id AS claim_id,
        c.status AS claim_status,
        c.adjuster_id AS claim_adjuster_id,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at
    FROM sequenced s
    LEFT JOIN claims c ON c.policy_id = s.id AND c.status = 'APPROVED'
),
line_level_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.start_date,
        s.status,
        s.claim_id,
        s.claim_status,
        s.claim_adjuster_id,
        s.claim_reported_at,
        c.id AS claim_id_2,
        c.adjuster_id AS claim_adjuster_id_2,
        c.reported_at AS claim_reported_at_2
    FROM line_level s
    JOIN claims c ON c.policy_id = s.id
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    claim_id,
    claim_status,
    claim_adjuster_id,
    claim_reported_at,
    claim_id_2,
    claim_reported_at_2,
    LOWER(TRIM(claim_status)) AS claim_status_clean
FROM line_level_2
