WITH cleaned_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix,
        DATEDIFF(DATE '2025-06-30', end_date) AS days_since_end_date
    FROM policies
    WHERE status = 'ACTIVE'
      AND product = 'HOME'
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        *,
        RANK() OVER (PARTITION BY product ORDER BY days_since_end_date DESC) AS days_since_end_date_rank,
        LAG(days_since_end_date) OVER (PARTITION BY holder_id, status ORDER BY end_date DESC, id) AS prev_days_since_end_date,
        LAG(start_date) OVER (PARTITION BY holder_id, status ORDER BY end_date DESC, id) AS prev_start_date
    FROM cleaned_policys
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        COUNT(*) AS n_rows,
        MAX(start_date) AS last_start_date,
        ROUND(SUM(prev_days_since_end_date) / NULLIF(SUM(days_since_end_date), 0), 3) AS prev_days_since_end_date_to_days_since_end_date_
    FROM sequenced
    WHERE product_prefix IS NOT NULL
    GROUP BY DATE_TRUNC('MONTH', end_date)
    HAVING COUNT(*) >= 3
)
-- Final projection
SELECT
    end_date_month,
    last_start_date,
    prev_days_since_end_date_to_days_since_end_date_
FROM totals
ORDER BY end_date_month, last_start_date
LIMIT 5
