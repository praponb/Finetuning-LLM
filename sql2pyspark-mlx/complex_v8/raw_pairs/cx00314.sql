WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        CASE WHEN status IN ('ACTIVE', 'CANCELLED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group,
        LOWER(TRIM(status)) AS status_clean
    FROM policies
    WHERE status = 'CANCELLED'
      AND product = 'AUTO'
),
enriched AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status_group,
        status_clean,
        COALESCE(premium, 0.0) AS premium_filled
    FROM recent_policys
    WHERE status_clean IS NOT NULL
),
tagged AS (
    -- Add calculated columns
    SELECT
        *,
        DATEDIFF(DATE '2026-01-01', start_date) AS days_since_start_date,
        CONCAT_WS('-', product, status_group) AS product_status_group_label
    FROM enriched
    WHERE status_group = 'group_b'
),
scored AS (
    -- Derive helper columns
    SELECT
        *,
        premium_filled - days_since_start_date AS premium_filled_minus_days_since_start_date,
        ROUND(premium_filled * 1.07, 2) AS premium_filled_adj,
        premium_filled + days_since_start_date AS premium_filled_plus_days_since_start_date
    FROM tagged
    WHERE start_date < DATE '2025-05-30'
)
SELECT *
FROM (
    SELECT end_date, product, premium_filled
    FROM scored
) src
PIVOT (
    AVG(premium_filled) AS avg_v, SUM(premium_filled) AS sum_v
    FOR product IN ('HOME', 'LIFE', 'AUTO', 'HEALTH')
)
