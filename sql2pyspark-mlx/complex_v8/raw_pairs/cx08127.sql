WITH valid AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        LENGTH(TRIM(product)) AS product_len,
        DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date
    FROM policies
    WHERE product IN ('HEALTH', 'HOME')
),
unique_rows AS (
    -- Keep the earliest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        product_len,
        days_since_start_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY end_date, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
annotated AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        start_date,
        product_len,
        days_since_start_date,
        LOWER(REGEXP_REPLACE(TRIM(product), ' ', '_')) AS product_slug,
        COALESCE(days_since_start_date, 0) AS days_since_start_date_filled,
        CONCAT(product, '#', CAST(product_len AS STRING)) AS product_ref
    FROM unique_rows
    WHERE product_len >= 6
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(days_since_start_date - AVG(days_since_start_date) OVER (PARTITION BY holder_id, product_len), 2) AS days_since_start_date_vs_holder_id_product_len_a,
        ROUND(AVG(days_since_start_date) OVER (PARTITION BY holder_id, product_len), 2) AS holder_id_product_len_avg_days_since_start_date,
        ROW_NUMBER() OVER (PARTITION BY product_len ORDER BY days_since_start_date_filled ASC NULLS LAST, id) AS rn
    FROM annotated
),
combined_details AS (
    -- Attach holder attributes
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.product_len,
        s.product_slug,
        s.days_since_start_date_filled,
        s.product_ref,
        s.days_since_start_date_vs_holder_id_product_len_a,
        s.holder_id_product_len_avg_days_since_start_date,
        s.rn,
        p.segment AS holder_segment
    FROM sequenced s
    JOIN policyholders p ON p.id = s.holder_id AND p.risk_score >= 0.2
),
grouped AS (
    -- Summarise per group
    SELECT
        product_len,
        holder_segment,
        DATEDIFF(MAX(start_date), MIN(start_date)) AS start_date_span_days,
        MAX(days_since_start_date_filled) AS max_days_since_start_date_filled
    FROM combined_details
    GROUP BY 1, 2
    HAVING SUM(rn) > 2
)
SELECT product_len, holder_segment, start_date_span_days
FROM grouped
WHERE start_date_span_days <= 630
