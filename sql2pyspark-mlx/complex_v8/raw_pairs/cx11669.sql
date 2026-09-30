WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM policies
    WHERE premium <= 2844.1
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.*,
        p.region AS holder_region,
        p.holder_name,
        p.risk_score AS holder_risk_score
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        status_slug,
        product_ref,
        holder_region,
        holder_risk_score,
        COUNT(*) OVER w_end_date_month_start_date_ye AS rows_in_end_date_month_start_date_year,
        ROUND(AVG(holder_risk_score) OVER (PARTITION BY MONTH(end_date), YEAR(start_date)), 2) AS end_date_month_start_date_year_avg_holder_risk_s,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (PARTITION BY holder_region ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        ROUND(AVG(premium) OVER (PARTITION BY holder_region ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4
    FROM with_parent
    WINDOW w_end_date_month_start_date_ye AS (PARTITION BY MONTH(end_date), YEAR(start_date))
),
detail_rows_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.start_date,
        s.end_date,
        s.status_slug,
        s.holder_region,
        s.rows_in_end_date_month_start_date_year,
        s.first_known_end_date,
        s.premium_avg_4,
        c.id AS claim_id,
        c.approved_amount AS claim_approved_amount,
        c.claimed_amount AS claim_claimed_amount,
        c.reported_at AS claim_reported_at,
        c.claim_date
    FROM windowed s
    JOIN claims c ON c.policy_id = s.id
)
-- Subtotals with GROUPING SETS
SELECT
    holder_id,
    holder_region,
    SUM(claim_claimed_amount) FILTER (WHERE holder_region <> 'South') AS claim_claimed_amount_holder_region_south,
    COUNT(*) FILTER (WHERE holder_region IN ('West', 'North')) AS n_holder_region_in_west_north
FROM detail_rows_claims
GROUP BY GROUPING SETS ((holder_id, holder_region), (holder_id), ())
