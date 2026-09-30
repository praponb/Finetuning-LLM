WITH base AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE status = 'LAPSED'
      AND start_date IS NOT NULL
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        SUM(premium) OVER (PARTITION BY status) AS status_sum_premium,
        NTILE(5) OVER (PARTITION BY holder_id ORDER BY start_date, id) AS start_date_quintile,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY premium DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        FIRST_VALUE(start_date) OVER (PARTITION BY holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_holder_id_by_premium_2) AS days_since_prev_start_date,
        ROW_NUMBER() OVER (PARTITION BY status, holder_id ORDER BY premium DESC NULLS FIRST, id) AS rn,
        RANK() OVER w_status_holder_id_by_premium_2 AS premium_rank,
        ROUND(AVG(premium) OVER w_status, 2) AS status_avg_premium
    FROM base
    WINDOW w_status_holder_id_by_premium_2 AS (PARTITION BY status, holder_id ORDER BY premium DESC NULLS FIRST, id), w_status AS (PARTITION BY status)
),
annotated AS (
    -- Label and score rows
    SELECT
        id,
        premium,
        start_date,
        status,
        running_premium,
        last_known_premium,
        first_start_date,
        DATEDIFF(start_date, first_start_date) AS days_first_start_date_to_start_date
    FROM windowed
)
-- Final projection
SELECT
    id,
    premium,
    start_date,
    status,
    running_premium,
    last_known_premium,
    first_start_date,
    days_first_start_date_to_start_date,
    COALESCE(status, 'UNKNOWN') AS status_or_unknown,
    CONCAT(status, '#', CAST(id AS STRING)) AS status_ref
FROM annotated
ORDER BY premium DESC NULLS FIRST
