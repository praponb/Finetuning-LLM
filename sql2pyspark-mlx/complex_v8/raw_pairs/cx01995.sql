WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        DATEDIFF(DATE '2025-06-30', start_date) AS days_since_start_date
    FROM policies
    WHERE holder_id IS NOT NULL
      AND product <> 'LIFE'
),
with_parent_holder AS (
    -- Join policyholders
    SELECT
        s.id,
        s.holder_id,
        s.product,
        s.premium,
        s.start_date,
        s.product_or_unknown,
        s.days_since_start_date,
        p.holder_name
    FROM cleaned s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE p.holder_name IS NOT NULL
)
SELECT *
FROM (
    SELECT product, product_or_unknown, premium
    FROM with_parent_holder
) src
PIVOT (
    AVG(premium)
    FOR product_or_unknown IN ('LIFE', 'HEALTH', 'TRAVEL', 'AUTO')
)
