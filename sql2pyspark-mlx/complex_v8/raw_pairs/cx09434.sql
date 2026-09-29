WITH base_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        CONCAT_WS(': ', status, product) AS status_product_label,
        CONCAT_WS(': ', status, product) AS status_product_label_2,
        DATEDIFF(DATE '2025-12-31', end_date) AS days_since_end_date
    FROM policies
    WHERE product = 'AUTO'
),
scored AS (
    -- Label and score rows
    SELECT
        id,
        premium,
        end_date,
        days_since_end_date,
        CASE WHEN days_since_end_date >= 525 THEN 'large' WHEN days_since_end_date >= 245 THEN 'medium' ELSE 'small' END AS days_since_end_date_tier,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug,
        DAYOFWEEK(end_date) AS end_date_dayofweek
    FROM base_policys
    WHERE status_product_label_2 IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, amount
FROM (
    SELECT id, days_since_end_date, end_date_dayofweek
    FROM scored
) src
UNPIVOT (
    amount FOR field IN (days_since_end_date AS days_since_end_date_v, end_date_dayofweek AS m_end_date_dayofweek)
) unp
ORDER BY id, field
