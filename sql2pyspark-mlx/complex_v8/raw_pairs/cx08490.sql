WITH cleaned_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        UPPER(TRIM(status)) AS status_clean,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        DATE_TRUNC('YEAR', end_date) AS end_date_year
    FROM policies
    WHERE product = 'HOME'
),
with_children AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.status_clean,
        s.days_start_date_to_end_date,
        s.end_date_year,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at
    FROM cleaned_policys s
    JOIN claims c ON c.policy_id = s.id
),
screened AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_children s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status = 'ACTIVE'
              AND p.product IS NOT NULL
        )
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        id,
        holder_id,
        premium,
        status_clean,
        end_date_year,
        claim_id,
        claim_status,
        ROUND(AVG(premium) OVER (PARTITION BY claim_status, holder_id ORDER BY claim_reported_at, id, claim_id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_2_prior,
        MIN(days_start_date_to_end_date) OVER (PARTITION BY claim_status ORDER BY claim_reported_at, id, claim_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_days_start_date_to_end_date_so_far,
        ROUND(PERCENT_RANK() OVER (PARTITION BY claim_status ORDER BY claim_reported_at), 3) AS claim_reported_at_percent_rank,
        ROUND((UNIX_TIMESTAMP(end_date_year) - UNIX_TIMESTAMP(LAG(end_date_year) OVER (PARTITION BY claim_status ORDER BY claim_reported_at, id, claim_id))) / 60, 1) AS minutes_since_prev_end_date_year,
        ROUND(claim_approved_amount - AVG(claim_approved_amount) OVER (PARTITION BY holder_id, claim_id), 2) AS claim_approved_amount_vs_holder_id_claim_id_avg
    FROM screened
),
with_children_2 AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date_year,
        s.claim_id,
        s.claim_status,
        s.premium_avg_2_prior,
        s.min_days_start_date_to_end_date_so_far,
        s.claim_reported_at_percent_rank,
        s.minutes_since_prev_end_date_year,
        s.claim_approved_amount_vs_holder_id_claim_id_avg,
        c.id AS claim_id_2,
        c.claim_type,
        c.status AS claim_status_2
    FROM metrics s
    JOIN claims c ON c.policy_id = s.id AND c.status IN ('IN_REVIEW', 'OPEN')
),
filtered AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM with_children_2 s
    WHERE s.id IN (
            SELECT c.policy_id
            FROM claims c
        )
),
qualified AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM filtered s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
              AND c.claim_date >= DATE '2024-04-11'
              AND c.claimed_amount BETWEEN 3192.1 AND 32441.2
        )
      AND s.claim_approved_amount_vs_holder_id_claim_id_avg > -15000.0
)
SELECT id, claim_id, field, metric_value
FROM (
    SELECT id, claim_id, claim_approved_amount_vs_holder_id_claim_id_avg, minutes_since_prev_end_date_year, claim_reported_at_percent_rank
    FROM qualified
) src
UNPIVOT (
    metric_value FOR field IN (claim_approved_amount_vs_holder_id_claim_id_avg, minutes_since_prev_end_date_year, claim_reported_at_percent_rank)
) unp
