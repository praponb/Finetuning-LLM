WITH cleaned AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ROUND(premium * 1.07, 2) AS premium_adj,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE start_date < DATE '2025-07-07'
),
scored AS (
    -- Label and score rows
    SELECT
        *,
        ROUND(premium * 0.25, 2) AS premium_adj_2,
        MONTH(start_date) AS start_date_month,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM cleaned
),
with_activity AS (
    -- Rows that have matching claims
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM claims c
            WHERE c.policy_id = s.id
        )
      AND s.premium_adj_2 > 2390.0
)
SELECT
    start_date_month,
    COALESCE(status, 'ALL') AS status,
    COALESCE(product, 'ALL') AS product,
    ROUND(STDDEV(premium_adj), 2) AS stddev_premium_adj
FROM with_activity
GROUP BY ROLLUP(start_date_month, status, product)
ORDER BY start_date_month NULLS LAST, status NULLS LAST, product NULLS LAST
