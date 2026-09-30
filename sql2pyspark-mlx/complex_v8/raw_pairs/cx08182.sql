WITH filtered_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status
    FROM policies
    WHERE status NOT IN ('ACTIVE', 'EXPIRED')
      AND holder_id >= 17
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        premium,
        end_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY status ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        premium - LAG(premium) OVER w_status_by_end_date AS premium_change,
        MIN(premium) OVER w_holder_id AS holder_id_min_premium,
        NTH_VALUE(premium, 2) OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        SIZE(COLLECT_SET(status) OVER w_holder_id) AS distinct_status_in_holder_id,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        FIRST_VALUE(end_date) OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        SUM(premium) OVER (PARTITION BY status ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        FIRST_VALUE(premium) OVER (PARTITION BY status ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_premium,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id), 0), 4) AS premium_share_of_holder_id,
        SUM(premium) OVER (ORDER BY end_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium_2,
        LAG(premium) OVER (PARTITION BY status ORDER BY premium, id) AS prev_premium
    FROM filtered_policys
    WINDOW w_status_by_end_date AS (PARTITION BY status ORDER BY end_date ASC, id), w_holder_id AS (PARTITION BY holder_id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(holder_id_min_premium, 2) OVER (PARTITION BY distinct_status_in_holder_id, DATE_TRUNC('MONTH', second_end_date) ORDER BY first_premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_holder_id_min_premium,
        ROUND(distinct_status_in_holder_id - AVG(distinct_status_in_holder_id) OVER w_second_end_date_year, 2) AS distinct_status_in_holder_id_vs_second_end_date_,
        MAX(premium_vs_holder_id_avg) OVER (PARTITION BY YEAR(second_end_date)) AS second_end_date_year_max_premium_vs_holder_id_av,
        ROW_NUMBER() OVER w_all_by_holder_id_min_premium AS holder_id_min_premium_seq,
        RANK() OVER (PARTITION BY distinct_status_in_holder_id, DATE_TRUNC('MONTH', second_end_date) ORDER BY first_premium DESC, id) AS first_premium_rank,
        LEAD(running_premium, 2) OVER (ORDER BY holder_id_min_premium DESC, id) AS next2_running_premium,
        MAX(premium_change) OVER w_second_end_date_year AS second_end_date_year_max_premium_change
    FROM with_windows
    WINDOW w_second_end_date_year AS (PARTITION BY YEAR(second_end_date)), w_all_by_holder_id_min_premium AS (ORDER BY holder_id_min_premium DESC, id)
),
totals AS (
    -- Group-level totals
    SELECT
        DATE_FORMAT(end_date, 'yyyy-MM') AS end_date_ym,
        COUNT(DISTINCT id) AS distinct_ids,
        COUNT(DISTINCT product) AS distinct_products,
        MIN(premium) AS min_premium,
        MAX(second_end_date_year_max_premium_change) AS max_second_end_date_year_max_premium_change,
        COUNT(*) AS n_rows
    FROM sequenced
    WHERE premium <= 3532.7
    GROUP BY DATE_FORMAT(end_date, 'yyyy-MM')
)
SELECT
    end_date_ym,
    distinct_ids,
    distinct_products,
    min_premium,
    max_second_end_date_year_max_premium_change,
    n_rows,
    FLOOR(distinct_products / 2) * 2 AS distinct_products_bucket,
    ROUND(distinct_products / NULLIF(distinct_ids, 0), 3) AS distinct_products_per_distinct_ids
FROM totals
ORDER BY distinct_products_per_distinct_ids DESC
