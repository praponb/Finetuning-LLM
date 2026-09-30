WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product IS NOT NULL
      AND status = 'CANCELLED'
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        holder_id,
        MIN(premium) AS min_premium,
        SUM(CASE WHEN product NOT IN ('AUTO', 'LIFE') THEN 1 ELSE 0 END) AS n_product_not_in_auto_life,
        SUM(premium) FILTER (WHERE status = 'EXPIRED') AS premium_status_expired,
        COUNT(*) AS n_rows
    FROM base
    GROUP BY DATE_TRUNC('MONTH', start_date), holder_id
)
SELECT start_date_month, holder_id, measure, metric_value
FROM (
    SELECT start_date_month, holder_id, n_rows, n_product_not_in_auto_life
    FROM summary
) src
UNPIVOT (
    metric_value FOR measure IN (n_rows AS n_rows_v, n_product_not_in_auto_life AS m_n_product_not_in_auto_life)
) unp
