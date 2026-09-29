WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE start_date BETWEEN DATE '2024-11-20' AND DATE '2025-08-03'
),
rollup_base AS (
    -- Aggregate with conditional metrics
    SELECT
        holder_id,
        status,
        ROUND(SUM(days_start_date_to_end_date) / NULLIF(SUM(premium), 0), 3) AS days_start_date_to_end_date_to_premium_ratio,
        MIN(end_date) AS first_end_date,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list,
        ROUND(SUM(days_since_end_date) / NULLIF(SUM(days_start_date_to_end_date), 0), 3) AS days_since_end_date_to_days_start_date_to_end_da,
        COUNT(*) AS n_rows
    FROM valid_policys
    WHERE days_since_end_date >= 70
    GROUP BY holder_id, status
),
exploded_detail_policys AS (
    -- One row per policy
    SELECT
        s.holder_id,
        s.status,
        s.n_rows,
        p.id AS policy_id,
        p.start_date AS policy_start_date,
        p.status AS policy_status,
        p.premium AS policy_premium,
        p.end_date AS policy_end_date
    FROM rollup_base s
    JOIN policies p ON p.holder_id = s.holder_id AND p.start_date BETWEEN DATE '2024-08-27' AND DATE '2025-09-24'
),
scored AS (
    -- Derive helper columns
    SELECT
        holder_id,
        status,
        n_rows,
        policy_id,
        policy_start_date,
        policy_premium,
        policy_end_date,
        TRUNC(policy_start_date, 'MM') AS policy_start_date_month_start,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        CONCAT(policy_status, '#', CAST(policy_id AS STRING)) AS policy_status_ref
    FROM exploded_detail_policys
),
without_matches AS (
    -- Rows with no matching claims
    SELECT s.*
    FROM scored s
    WHERE NOT EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.policy_id
              AND c.claim_type IN ('delay', 'collision')
              AND c.claim_date < DATE '2024-12-22'
        )
      AND s.status_ref IS NOT NULL
)
-- Native SQL PIVOT (long to wide)
SELECT *
FROM (
    SELECT policy_id, status, n_rows
    FROM without_matches
) src
PIVOT (
    AVG(n_rows)
    FOR status IN ('EXPIRED', 'ACTIVE', 'LAPSED', 'CANCELLED')
)
ORDER BY policy_id
