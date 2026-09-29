WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        SUBSTRING(TRIM(status), 1, 3) AS status_prefix,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM policies
),
grouped AS (
    -- Summarise per group
    SELECT
        holder_id,
        SUM(premium) AS total_premium,
        SUM(days_start_date_to_end_date) AS total_days_start_date_to_end_date,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids
    FROM prepared_policys
    GROUP BY holder_id
),
without_matches AS (
    -- Rows that have matching policies
    SELECT s.*
    FROM grouped s
    WHERE EXISTS (
            SELECT 1
            FROM policies p
            WHERE p.holder_id = s.holder_id
              AND p.status IN ('CANCELLED', 'LAPSED')
              AND p.end_date BETWEEN DATE '2026-01-23' AND DATE '2026-11-25'
        )
),
with_details_holder AS (
    -- Join policyholders
    SELECT
        s.holder_id,
        s.total_premium,
        s.distinct_holder_ids,
        p.risk_score AS holder_risk_score,
        p.region AS holder_region
    FROM without_matches s
    LEFT JOIN policyholders p ON p.id = s.holder_id
),
unique_rows AS (
    -- Keep the latest row per holder_region
    SELECT
        holder_id,
        total_premium,
        distinct_holder_ids,
        holder_risk_score,
        holder_region
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_region ORDER BY total_premium DESC, holder_id DESC) AS rn
        FROM with_details_holder
    ) ranked
    WHERE rn = 1
)
SELECT
    distinct_holder_ids,
    holder_region,
    holder_id,
    COUNT(DISTINCT holder_id) AS distinct_holder_ids_2,
    COUNT(*) AS cnt
FROM unique_rows
GROUP BY GROUPING SETS ((distinct_holder_ids, holder_region, holder_id), (distinct_holder_ids), ())
ORDER BY distinct_holder_ids NULLS LAST, holder_region NULLS LAST, holder_id NULLS LAST
