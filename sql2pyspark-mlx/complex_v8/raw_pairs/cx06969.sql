WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        ABS(premium - 2319.8) AS premium_distance,
        COALESCE(premium, 0.0) AS premium_filled,
        DATE_TRUNC('QUARTER', start_date) AS start_date_quarter
    FROM policies
    WHERE product IN ('HEALTH', 'HOME', 'AUTO')
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), holder_id ORDER BY premium, id) AS seq_no,
        COUNT(*) OVER (PARTITION BY TO_DATE(start_date_quarter), holder_id) AS rows_in_start_date_quarter_to_date_holder_id
    FROM recent_policys
)
SELECT id, field, metric_value
FROM (
    SELECT id, premium_distance, premium_filled, premium
    FROM windowed
) src
UNPIVOT (
    metric_value FOR field IN (premium_distance, premium_filled, premium)
) unp
ORDER BY id, field
