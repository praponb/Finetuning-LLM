WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        LAST_DAY(start_date) AS start_date_month_end
    FROM policies
    WHERE (status = 'EXPIRED' OR YEAR(end_date) = 2026)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        premium,
        status,
        status_len,
        start_date_month_end,
        MAX(status_len) OVER (PARTITION BY status, product ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS status_len_max_2_prior,
        MAX(premium) OVER (PARTITION BY MONTH(start_date_month_end) ORDER BY status_len, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS premium_max_6_prior,
        DENSE_RANK() OVER (PARTITION BY holder_id, MONTH(start_date_month_end) ORDER BY start_date ASC NULLS LAST) AS start_date_dense_rank,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status_len, holder_id), 2) AS premium_vs_status_len_holder_id_avg,
        DATEDIFF(start_date, LAG(start_date) OVER w_status_product_by_premium) AS days_since_prev_start_date,
        COUNT(*) OVER (PARTITION BY YEAR(start_date)) AS rows_in_start_date_year,
        LEAD(start_date) OVER w_start_date_month_end_month_p AS next_start_date,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY YEAR(start_date))) AS distinct_status_in_start_date_year,
        SUM(premium) OVER (PARTITION BY YEAR(start_date)) AS start_date_year_sum_premium,
        NTILE(3) OVER (PARTITION BY status_len, holder_id ORDER BY premium, id) AS premium_tercile,
        ROUND(CUME_DIST() OVER w_holder_id_start_date_month_e_2, 3) AS start_date_cume_dist
    FROM prepared_policys
    WINDOW w_status_product_by_premium AS (PARTITION BY status, product ORDER BY premium DESC, id), w_start_date_month_end_month_p AS (PARTITION BY DATE_TRUNC('MONTH', start_date_month_end), product ORDER BY start_date, id), w_holder_id_start_date_month_e_2 AS (PARTITION BY holder_id, MONTH(start_date_month_end) ORDER BY start_date ASC NULLS LAST, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        premium,
        status,
        status_len,
        premium_max_6_prior,
        start_date_dense_rank,
        days_since_prev_start_date,
        rows_in_start_date_year,
        next_start_date,
        start_date_year_sum_premium,
        start_date_cume_dist,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY premium_tercile), 0), 4) AS premium_share_of_premium_tercile,
        ROUND(AVG(premium) OVER (ORDER BY start_date_cume_dist, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        ROUND(start_date_dense_rank / NULLIF(SUM(start_date_dense_rank) OVER w_premium_tercile, 0), 4) AS start_date_dense_rank_share_of_premium_tercile,
        MIN(start_date_cume_dist) OVER (PARTITION BY premium_tercile) AS premium_tercile_min_start_date_cume_dist,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY premium_tercile)) AS distinct_status_in_premium_tercile,
        MIN(start_date_year_sum_premium) OVER (PARTITION BY premium_tercile) AS premium_tercile_min_start_date_year_sum_premium,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date_cume_dist), 3) AS start_date_cume_dist_percent_rank,
        MAX(start_date_cume_dist) OVER (PARTITION BY premium_tercile) AS premium_tercile_max_start_date_cume_dist
    FROM windowed
    WINDOW w_premium_tercile AS (PARTITION BY premium_tercile)
)
SELECT
    id,
    holder_id,
    premium,
    status,
    status_len,
    start_date_dense_rank,
    days_since_prev_start_date,
    rows_in_start_date_year,
    next_start_date,
    start_date_year_sum_premium,
    start_date_cume_dist,
    premium_avg_7,
    start_date_dense_rank_share_of_premium_tercile,
    premium_tercile_min_start_date_cume_dist,
    distinct_status_in_premium_tercile,
    premium_tercile_min_start_date_year_sum_premium,
    start_date_cume_dist_percent_rank,
    premium_tercile_max_start_date_cume_dist
FROM sequenced
ORDER BY start_date_cume_dist NULLS LAST, id
LIMIT 10
