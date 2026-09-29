WITH filtered_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        SUBSTRING(TRIM(product), 1, 2) AS product_prefix
    FROM policies
    WHERE holder_id >= 10
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status_or_unknown,
        product_prefix,
        DENSE_RANK() OVER w_end_date_month_status_or_unk AS premium_dense_rank,
        SIZE(COLLECT_SET(status_or_unknown) OVER (PARTITION BY holder_id)) AS distinct_status_or_unknown_in_holder_id,
        ROUND(premium - AVG(premium) OVER w_holder_id, 2) AS premium_vs_holder_id_avg,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id), 2) AS holder_id_avg_premium,
        LAST_VALUE(premium) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), status_or_unknown ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium,
        MAX(premium) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS premium_max_4,
        COUNT(*) OVER w_holder_id AS rows_in_holder_id,
        ROUND(AVG(premium) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        ROUND(CUME_DIST() OVER w_status_or_unknown_by_premium, 3) AS premium_cume_dist,
        MIN(premium) OVER (PARTITION BY holder_id) AS holder_id_min_premium,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_id)) AS distinct_status_in_holder_id,
        LEAD(premium) OVER (PARTITION BY status_or_unknown ORDER BY premium DESC NULLS FIRST, id) AS next_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id), 0), 4) AS premium_share_of_holder_id
    FROM filtered_policys
    WINDOW w_end_date_month_status_or_unk AS (PARTITION BY DATE_TRUNC('MONTH', end_date), status_or_unknown ORDER BY premium ASC, id), w_holder_id AS (PARTITION BY holder_id), w_status_or_unknown_by_premium AS (PARTITION BY status_or_unknown ORDER BY premium DESC NULLS FIRST)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        ADD_MONTHS(start_date, 3) AS start_date_plus_3m,
        CASE WHEN premium_max_4 >= 10560.0 THEN 'A' WHEN premium_max_4 >= 6120.0 THEN 'B' ELSE 'C' END AS premium_max_4_tier,
        holder_id_avg_premium - rows_in_holder_id AS holder_id_avg_premium_minus_rows_in_holder_id
    FROM with_windows
)
SELECT
    id,
    holder_id,
    product,
    premium,
    start_date,
    status_or_unknown,
    product_prefix,
    premium_dense_rank,
    distinct_status_or_unknown_in_holder_id,
    premium_vs_holder_id_avg,
    holder_id_avg_premium,
    last_premium,
    premium_max_4,
    rows_in_holder_id,
    holder_id_min_premium,
    distinct_status_in_holder_id,
    next_premium,
    holder_id_avg_premium_minus_rows_in_holder_id
FROM labeled
ORDER BY holder_id_avg_premium_minus_rows_in_holder_id DESC
