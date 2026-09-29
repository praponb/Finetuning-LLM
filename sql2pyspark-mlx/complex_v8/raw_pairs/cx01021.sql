WITH recent_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        DATE_TRUNC('YEAR', start_date) AS start_date_year,
        CASE WHEN status IN ('ACTIVE', 'LAPSED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE status = 'CANCELLED'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SIZE(COLLECT_SET(product) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date_year))) AS distinct_product_in_holder_id_start_date_year_mo,
        SUM(premium) OVER (PARTITION BY status, status_group ORDER BY start_date_year DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_sum_2_prior,
        MIN(premium) OVER (PARTITION BY holder_id ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_premium_so_far,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY product), 0), 4) AS premium_share_of_product,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status, status_group ORDER BY start_date_year DESC, id), 3) AS start_date_year_percent_rank,
        LEAD(premium) OVER (PARTITION BY holder_id ORDER BY end_date, id) AS next_premium,
        COUNT(*) OVER w_product AS rows_in_product,
        LEAD(start_date_year, 2) OVER w_status_status_group_by_start_2 AS next2_start_date_year,
        MIN(premium) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', start_date_year)) AS holder_id_start_date_year_month_min_premium,
        NTILE(3) OVER w_holder_id_status_group_by_st AS start_date_year_tercile
    FROM recent_policys
    WINDOW w_product AS (PARTITION BY product), w_status_status_group_by_start_2 AS (PARTITION BY status, status_group ORDER BY start_date_year DESC, id), w_holder_id_status_group_by_st AS (PARTITION BY holder_id, status_group ORDER BY start_date_year ASC, id)
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        ADD_MONTHS(end_date, 6) AS end_date_plus_6m,
        ABS(premium - 2111.3) AS premium_distance,
        FLOOR(premium / 1000) * 1000 AS premium_bucket
    FROM metrics
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    premium,
    end_date,
    status,
    status_group,
    distinct_product_in_holder_id_start_date_year_mo,
    premium_share_of_product,
    start_date_year_percent_rank,
    rows_in_product,
    next2_start_date_year,
    start_date_year_tercile,
    end_date_plus_6m,
    premium_distance,
    premium_bucket
FROM labeled
ORDER BY status_group DESC, distinct_product_in_holder_id_start_date_year_mo DESC NULLS FIRST
