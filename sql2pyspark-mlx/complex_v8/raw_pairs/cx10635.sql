WITH recent_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LOWER(TRIM(product)) AS product_clean
    FROM policies
    WHERE holder_id IS NOT NULL
      AND end_date >= DATE '2026-01-23'
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(PERCENT_RANK() OVER (PARTITION BY holder_id ORDER BY premium DESC, id), 3) AS premium_percent_rank,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY premium DESC, id), 3) AS premium_cume_dist,
        ROUND(AVG(premium) OVER (ORDER BY premium ASC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW), 2) AS premium_avg_4,
        MAX(premium) OVER (PARTITION BY product ORDER BY end_date NULLS LAST, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        NTH_VALUE(start_date, 2) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY holder_id ORDER BY premium DESC, id)) AS days_since_prev_start_date,
        MAX(premium) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS premium_max_2_2,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY product ORDER BY end_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date_2
    FROM recent_policys
),
labeled AS (
    -- Derive helper columns
    SELECT
        *,
        CONCAT_WS(': ', product, status) AS product_status_label,
        DATE_FORMAT(last_known_start_date, 'MMM yyyy') AS last_known_start_date_label
    FROM sequenced
)
SELECT
    id,
    product,
    start_date,
    end_date,
    status,
    premium_percent_rank,
    premium_cume_dist,
    last_known_start_date,
    days_since_prev_start_date,
    premium_max_2_2,
    second_start_date_2,
    last_known_start_date_label,
    ADD_MONTHS(start_date, 3) AS start_date_plus_3m,
    ROUND(premium_avg_4 * 1.1, 2) AS premium_avg_4_adj
FROM labeled
