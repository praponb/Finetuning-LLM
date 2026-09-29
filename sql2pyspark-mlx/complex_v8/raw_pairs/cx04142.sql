WITH prepared_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product = 'HEALTH'
),
with_parent AS (
    -- Attach holder attributes
    SELECT s.*, p.holder_name
    FROM prepared_policys s
    JOIN policyholders p ON p.id = s.holder_id
    WHERE UPPER(TRIM(p.holder_name)) LIKE 'D%'
)
SELECT
    holder_id,
    status,
    product,
    ROUND(AVG(premium), 2) AS avg_premium,
    ARRAY_SORT(COLLECT_SET(product)) AS product_list
FROM with_parent
GROUP BY GROUPING SETS ((holder_id, status, product), (holder_id), (status), ())
