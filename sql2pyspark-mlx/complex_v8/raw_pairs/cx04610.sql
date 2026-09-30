WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        DATEDIFF(start_date, end_date) AS days_end_date_to_start_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        CASE WHEN product IN ('HOME', 'AUTO') THEN 'group_a' WHEN product = 'LIFE' THEN 'group_b' ELSE 'other' END AS product_group
    FROM policies
    WHERE (holder_id >= 6 OR start_date < DATE '2025-03-24')
),
with_counts AS (
    -- Attach claims metrics
    SELECT
        s.*,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_approved_amount,
        (SELECT COUNT(*) FROM claims c WHERE c.policy_id = s.id) AS claim_count,
        (SELECT SUM(c.approved_amount) FROM claims c WHERE c.policy_id = s.id) AS total_claim_approved_amount_2
    FROM scoped s
),
qualified AS (
    -- Rows with no matching policies
    SELECT s.*
    FROM with_counts s
    WHERE NOT EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.premium < 4417.0
        )
),
tagged AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        days_end_date_to_start_date,
        status_slug,
        total_claim_approved_amount,
        claim_count,
        YEAR(start_date) AS start_date_year
    FROM qualified
),
grouped AS (
    -- Aggregate with conditional metrics
    SELECT
        status,
        product,
        start_date_year,
        COUNT(*) FILTER (WHERE status IS NOT NULL) AS n_status_is_not_null,
        SUM(claim_count) AS total_claim_count,
        SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) AS n_status_active,
        COUNT(*) FILTER (WHERE status IN ('LAPSED', 'EXPIRED', 'CANCELLED')) AS n_status_in_lapsed_expired_can
    FROM tagged
    WHERE holder_id IS NOT NULL
    GROUP BY status, product, start_date_year
)
-- Final projection
SELECT
    status,
    product,
    start_date_year,
    n_status_is_not_null,
    n_status_in_lapsed_expired_can
FROM grouped
ORDER BY product DESC, status DESC NULLS FIRST, start_date_year
LIMIT 20
