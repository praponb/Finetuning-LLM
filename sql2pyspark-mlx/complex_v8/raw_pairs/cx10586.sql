WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM policies
    WHERE holder_id IS NOT NULL
),
enriched AS (
    -- Derive helper columns
    SELECT
        *,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        DATE_TRUNC('MONTH', start_date) AS start_date_month
    FROM filtered_policys
),
enriched_2 AS (
    -- Add calculated columns
    SELECT *, LOWER(TRIM(product)) AS product_clean
    FROM enriched
    WHERE product IS NOT NULL
),
enriched_3 AS (
    -- Label and score rows
    SELECT
        *,
        CONCAT(product_clean, '#', CAST(end_date_dayofweek AS STRING)) AS product_clean_ref,
        LENGTH(TRIM(product_clean)) AS product_clean_len,
        CASE WHEN end_date_dayofweek >= 4 THEN 'A' WHEN end_date_dayofweek >= 2 THEN 'B' ELSE 'C' END AS end_date_dayofweek_tier
    FROM enriched_2
    WHERE product_or_unknown = 'TRAVEL'
)
-- Subtotals with GROUPING SETS
SELECT
    product_or_unknown,
    end_date_dayofweek_tier,
    GROUPING(product_or_unknown) AS is_total_product_or_unknown,
    ARRAY_SORT(COLLECT_SET(status)) AS status_list
FROM enriched_3
GROUP BY GROUPING SETS ((product_or_unknown, end_date_dayofweek_tier), (product_or_unknown))
ORDER BY product_or_unknown NULLS LAST, end_date_dayofweek_tier NULLS LAST
