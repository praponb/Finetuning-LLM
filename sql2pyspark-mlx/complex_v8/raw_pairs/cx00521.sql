WITH base AS (
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
    WHERE premium BETWEEN 918.1 AND 3544.1
      AND status = 'EXPIRED'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, status ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        MAX(premium) OVER (ORDER BY end_date, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_max_2_prior,
        ROUND(AVG(premium) OVER w_holder_id_status, 2) AS holder_id_status_avg_premium,
        MIN(premium) OVER (PARTITION BY holder_id, status ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        COUNT(*) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date)) AS rows_in_start_date_month,
        SUM(premium) OVER (PARTITION BY status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY status ORDER BY start_date DESC, id)) AS days_since_prev_end_date
    FROM base
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status)
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(running_premium / NULLIF(SUM(running_premium) OVER w_holder_id_status, 0), 4) AS running_premium_share_of_holder_id_status,
        ROW_NUMBER() OVER (PARTITION BY product, holder_id ORDER BY min_premium_so_far, id) AS rn,
        FIRST_VALUE(premium_max_2_prior) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', start_date) ORDER BY min_premium_so_far DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_max_2_prior
    FROM sequenced
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status)
),
stats AS (
    -- Summarise per group
    SELECT
        YEAR(start_date) AS start_date_year,
        DATE_TRUNC('MONTH', end_date) AS end_date_month,
        holder_id,
        COUNT(DISTINCT holder_id) AS distinct_holder_ids,
        MAX(running_premium_share_of_holder_id_status) AS max_running_premium_share_of_holder_id_status,
        COUNT(DISTINCT id) AS distinct_ids,
        ARRAY_SORT(COLLECT_SET(product)) AS product_list
    FROM windowed
    WHERE min_premium_so_far >= 5180.0
    GROUP BY 1, 2, 3
)
SELECT
    start_date_year,
    holder_id,
    distinct_holder_ids,
    max_running_premium_share_of_holder_id_status,
    distinct_ids,
    product_list
FROM stats
ORDER BY max_running_premium_share_of_holder_id_status DESC NULLS FIRST
