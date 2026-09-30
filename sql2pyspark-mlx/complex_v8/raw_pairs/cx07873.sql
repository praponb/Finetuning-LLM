WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        CONCAT(status, '#', CAST(holder_id AS STRING)) AS status_ref,
        ABS(premium - 3465.4) AS premium_distance,
        SUBSTRING(TRIM(status), 1, 2) AS status_prefix
    FROM policies
    WHERE start_date IS NOT NULL
),
with_trends AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        status_ref,
        premium_distance,
        SUM(premium) OVER (PARTITION BY holder_id, status) AS holder_id_status_sum_premium,
        SIZE(COLLECT_SET(product) OVER w_holder_id_status) AS distinct_product_in_holder_id_status
    FROM filtered
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status)
),
with_parent AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.premium,
        s.end_date,
        s.status,
        s.holder_id_status_sum_premium,
        s.distinct_product_in_holder_id_status,
        p.segment AS holder_segment,
        p.risk_score AS holder_risk_score
    FROM with_trends s
    JOIN policyholders p ON p.id = s.holder_id
)
SELECT
    id,
    holder_id,
    premium,
    status,
    holder_id_status_sum_premium,
    holder_segment
FROM with_parent
ORDER BY status DESC
