WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        SUBSTRING(TRIM(status), 1, 4) AS status_prefix,
        DATEDIFF(end_date, start_date) AS days_start_date_to_end_date
    FROM policies
    WHERE holder_id <= 19
      AND premium BETWEEN 918.1 AND 3817.5
),
unique_rows AS (
    -- Keep the earliest row per status
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        status_prefix,
        days_start_date_to_end_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status ORDER BY days_start_date_to_end_date, id) AS recency_rank
        FROM filtered_policys
    ) ranked
    WHERE recency_rank = 1
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        status_prefix,
        ROW_NUMBER() OVER (PARTITION BY product, status ORDER BY end_date ASC, id) AS rn,
        MAX(days_start_date_to_end_date) OVER w_start_date_month_status AS start_date_month_status_max_days_start_date_to_e
    FROM unique_rows
    WINDOW w_start_date_month_status AS (PARTITION BY DATE_TRUNC('MONTH', start_date), status)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status,
    status_prefix,
    rn,
    start_date_month_status_max_days_start_date_to_e,
    CONCAT(status, '#', CAST(rn AS STRING)) AS status_ref,
    ABS(rn - 5) AS rn_distance
FROM with_trends
ORDER BY start_date
