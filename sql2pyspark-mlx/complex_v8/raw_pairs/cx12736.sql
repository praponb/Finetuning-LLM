WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date,
        ABS(premium - 2111.3) AS premium_distance,
        DATE_ADD(end_date, 7) AS end_date_plus_7d
    FROM policies
    WHERE holder_id >= 5
      AND YEAR(end_date) = 2026
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        LENGTH(TRIM(status)) AS status_len,
        CONCAT_WS('-', product, status) AS product_status_label
    FROM filtered_policys
)
-- Subtotals with CUBE
SELECT
    status,
    holder_id,
    GROUPING(status) AS is_total_status,
    SUM(premium_distance) AS total_premium_distance
FROM enriched
GROUP BY CUBE(status, holder_id)
