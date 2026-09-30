WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        ABS(premium - 2844.1) AS premium_distance
    FROM policies
    WHERE product <> 'AUTO'
),
metrics AS (
    -- Window metrics over several partitions
    SELECT
        *,
        COUNT(*) OVER w_start_date_month AS rows_in_start_date_month,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY premium_distance, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date
    FROM filtered
    WINDOW w_start_date_month AS (PARTITION BY DATE_TRUNC('MONTH', start_date))
)
SELECT *
FROM (
    SELECT holder_id, start_date, product, premium
    FROM metrics
) src
PIVOT (
    SUM(premium) AS total, MAX(premium) AS max_v
    FOR product IN ('HEALTH', 'HOME')
)
ORDER BY holder_id, start_date
