WITH recent_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE (end_date >= DATE '2026-01-24' OR holder_id IS NOT NULL)
),
derived AS (
    -- Label and score rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref,
        LOWER(TRIM(status)) AS status_clean
    FROM recent_policys
    WHERE product <> 'HOME'
),
joined_holder AS (
    -- Enrich with dimension data
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.start_date,
        s.status,
        s.product_ref,
        s.status_clean,
        p.joined_on AS holder_joined_on,
        p.segment AS holder_segment,
        p.holder_name
    FROM derived s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE UPPER(TRIM(p.holder_name)) LIKE 'G%'
)
-- Subtotals with ROLLUP
SELECT
    holder_id,
    COALESCE(status, 'ALL') AS status,
    COALESCE(holder_segment, 'ALL') AS holder_segment,
    COUNT(*) FILTER (WHERE product = 'TRAVEL') AS n_product_travel
FROM joined_holder
GROUP BY ROLLUP(holder_id, status, holder_segment)
ORDER BY holder_id NULLS LAST, status NULLS LAST, holder_segment NULLS LAST
