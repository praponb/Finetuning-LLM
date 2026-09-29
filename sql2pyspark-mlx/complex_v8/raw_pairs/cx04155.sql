WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        ROUND(premium * 1.5, 2) AS premium_adj,
        UPPER(TRIM(status)) AS status_clean
    FROM policies
    WHERE (YEAR(start_date) = 2025 OR premium <= 3544.1)
),
tagged AS (
    -- Add calculated columns
    SELECT
        id,
        start_date,
        status,
        premium_adj,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        CONCAT_WS(' / ', status, product) AS status_product_label,
        ABS(premium_adj - 4945.0) AS premium_adj_distance
    FROM cleaned
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM') AS start_date_label,
        CASE WHEN product_or_unknown IN ('LIFE', 'TRAVEL') THEN 'group_a' WHEN product_or_unknown = 'HEALTH' THEN 'group_b' ELSE 'other' END AS product_or_unknown_group
    FROM tagged
)
-- Final projection
SELECT DISTINCT product_or_unknown
FROM labeled
ORDER BY product_or_unknown DESC
