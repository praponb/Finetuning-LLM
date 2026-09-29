WITH scoped_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT_WS(': ', status, product) AS status_product_label
    FROM policies
    WHERE holder_id <= 17
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(CUME_DIST() OVER (ORDER BY end_date ASC), 3) AS end_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY end_date ASC), 3) AS end_date_percent_rank,
        ROUND(CUME_DIST() OVER w_all_by_end_date, 3) AS end_date_cume_dist_2,
        NTH_VALUE(end_date, 2) OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROUND(PERCENT_RANK() OVER w_all_by_end_date, 3) AS end_date_percent_rank_2,
        LAST_VALUE(start_date) IGNORE NULLS OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date
    FROM scoped_policys
    WINDOW w_all_by_end_date AS (ORDER BY end_date ASC, id)
),
sequenced AS (
    -- Previous / next values per key
    SELECT
        id,
        holder_id,
        product,
        start_date,
        status,
        status_product_label,
        end_date_percent_rank,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product,
        ROUND(AVG(end_date_cume_dist_2) OVER (PARTITION BY product), 2) AS product_avg_end_date_cume_dist_2,
        ROUND(end_date_percent_rank - AVG(end_date_percent_rank) OVER (PARTITION BY product), 2) AS end_date_percent_rank_vs_product_avg
    FROM with_windows
)
SELECT *
FROM (
    SELECT holder_id, product, rows_in_product
    FROM sequenced
) src
PIVOT (
    MAX(rows_in_product)
    FOR product IN ('TRAVEL', 'HEALTH', 'AUTO', 'LIFE')
)
ORDER BY holder_id
