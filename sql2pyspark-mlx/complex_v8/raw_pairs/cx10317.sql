WITH prepared AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE start_date < DATE '2025-05-30'
),
with_stats AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT COUNT(CASE WHEN c.status IN ('APPROVED', 'IN_REVIEW') THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_status_count,
        (SELECT COUNT(CASE WHEN c.status = 'PAID' THEN 1 END) FROM claims c WHERE c.policy_id = s.id) AS claim_status_count_2
    FROM prepared s
),
derived AS (
    -- Label and score rows
    SELECT *, DATE_ADD(start_date, 7) AS start_date_plus_7d
    FROM with_stats
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.premium_filled,
        s.claim_status_count,
        s.claim_status_count_2,
        p.region AS holder_region,
        p.segment AS holder_segment,
        p.joined_on AS holder_joined_on
    FROM derived s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
exploded_detail_claims AS (
    -- One row per claim
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.premium_filled,
        s.claim_status_count,
        s.claim_status_count_2,
        s.holder_region,
        s.holder_segment,
        s.holder_joined_on,
        c.id AS claim_id,
        c.status AS claim_status,
        c.reported_at AS claim_reported_at,
        c.claim_type,
        c.approved_amount AS claim_approved_amount
    FROM joined_holder s
    JOIN claims c ON c.policy_id = s.id AND c.reported_at < TIMESTAMP '2025-02-24 07:00:00'
),
rollup_base AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(holder_joined_on, 'yyyy-MM') AS holder_joined_on_ym,
        YEAR(start_date) AS start_date_year,
        ARRAY_SORT(COLLECT_SET(holder_region)) AS holder_region_list,
        COUNT(*) AS row_count,
        ROUND(AVG(premium), 2) AS avg_premium
    FROM exploded_detail_claims
    WHERE premium_filled < 4280.0
    GROUP BY 1, 2
    HAVING COUNT(*) >= 2
),
qualified AS (
    -- Compare against the overall min
    SELECT *
    FROM rollup_base
    WHERE avg_premium > (SELECT MIN(avg_premium) FROM rollup_base)
)
-- Final projection
SELECT
    holder_joined_on_ym,
    start_date_year,
    row_count,
    LOWER(TRIM(holder_joined_on_ym)) AS holder_joined_on_ym_clean,
    ARRAY_JOIN(ARRAY_SORT(holder_region_list), ',') AS holder_region_list_text
FROM qualified
ORDER BY holder_region_list_text NULLS LAST, holder_joined_on_ym DESC, start_date_year
LIMIT 10
