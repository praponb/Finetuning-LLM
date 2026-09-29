WITH valid_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        ADD_MONTHS(start_date, 12) AS start_date_plus_12m,
        ABS(premium - 2289.0) AS premium_distance
    FROM policies
    WHERE start_date >= DATE '2024-08-27'
),
combined_details AS (
    -- Enrich with dimension data
    SELECT s.*, p.joined_on AS holder_joined_on, p.region AS holder_region
    FROM valid_policys s
    JOIN policyholders p ON p.id = s.holder_id AND LOWER(p.holder_name) LIKE '% f%'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(start_date_plus_12m, 3) OVER (PARTITION BY holder_id ORDER BY premium_distance DESC, id) AS next3_start_date_plus_12m,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY holder_id ORDER BY premium_distance DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        NTILE(3) OVER (PARTITION BY holder_region, product ORDER BY premium ASC, id) AS premium_tercile,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, status), 2) AS holder_id_status_avg_premium,
        DATEDIFF(start_date_plus_12m, LAG(start_date_plus_12m) OVER w_holder_id_by_premium_distanc_3) AS days_since_prev_start_date_plus_12m,
        COUNT(*) OVER (PARTITION BY holder_id, status) AS rows_in_holder_id_status,
        NTH_VALUE(premium, 2) OVER (PARTITION BY holder_region, product ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(AVG(premium_distance) OVER w_holder_id, 2) AS holder_id_avg_premium_distance
    FROM combined_details
    WINDOW w_holder_id_by_premium_distanc_3 AS (PARTITION BY holder_id ORDER BY premium_distance, id), w_holder_id AS (PARTITION BY holder_id)
),
annotated AS (
    -- Label and score rows
    SELECT
        *,
        ADD_MONTHS(holder_joined_on, 3) AS holder_joined_on_plus_3m,
        CONCAT_WS(': ', holder_region, product) AS holder_region_product_label
    FROM metrics
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date_plus_12m,
        premium_distance,
        holder_joined_on,
        holder_region,
        premium_tercile,
        holder_id_status_avg_premium,
        days_since_prev_start_date_plus_12m,
        rows_in_holder_id_status,
        second_premium,
        holder_joined_on_plus_3m,
        MAX(premium_distance) OVER (PARTITION BY premium_tercile ORDER BY premium_tercile DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_distance_so_far,
        MIN(second_premium) OVER (PARTITION BY status ORDER BY start_date_plus_12m, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_second_premium_so_far,
        ROUND(rows_in_holder_id_status / NULLIF(SUM(rows_in_holder_id_status) OVER (PARTITION BY holder_region, holder_id), 0), 4) AS rows_in_holder_id_status_share_of_holder_region_,
        ROUND(PERCENT_RANK() OVER (ORDER BY first_known_premium DESC), 3) AS first_known_premium_percent_rank,
        RANK() OVER (PARTITION BY premium_tercile ORDER BY premium_tercile DESC) AS premium_tercile_rank,
        SIZE(COLLECT_SET(status) OVER w_holder_region_holder_id) AS distinct_status_in_holder_region_holder_id
    FROM annotated
    WINDOW w_holder_region_holder_id AS (PARTITION BY holder_region, holder_id)
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date_plus_12m,
    premium_distance,
    holder_joined_on,
    holder_region,
    premium_tercile,
    holder_id_status_avg_premium,
    days_since_prev_start_date_plus_12m,
    rows_in_holder_id_status,
    second_premium,
    max_premium_distance_so_far,
    min_second_premium_so_far,
    rows_in_holder_id_status_share_of_holder_region_,
    first_known_premium_percent_rank,
    premium_tercile_rank,
    COALESCE(product, 'UNKNOWN') AS product_or_unknown
FROM sequenced
