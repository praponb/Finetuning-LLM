WITH recent AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        CONCAT(product, '#', CAST(id AS STRING)) AS product_ref
    FROM policies
    WHERE product = 'AUTO'
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        premium,
        end_date,
        status,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, status), 2) AS holder_id_status_avg_premium,
        MAX(premium) OVER (PARTITION BY YEAR(end_date)) AS end_date_year_max_premium,
        MAX(premium) OVER w_holder_id_status AS holder_id_status_max_premium,
        NTILE(4) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id ORDER BY end_date NULLS LAST, id) AS end_date_quartile,
        ROUND(AVG(premium) OVER (PARTITION BY YEAR(end_date)), 2) AS end_date_year_avg_premium,
        FIRST_VALUE(end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id ORDER BY end_date NULLS LAST, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_premium,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id, status ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY YEAR(end_date)), 0), 4) AS premium_share_of_end_date_year,
        ROUND(AVG(premium) OVER (PARTITION BY status ORDER BY premium DESC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS premium_avg_7,
        ROUND(PERCENT_RANK() OVER w_holder_id_by_premium_ties, 3) AS premium_percent_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY YEAR(end_date))) AS distinct_status_in_end_date_year,
        ROUND(AVG(premium) OVER (PARTITION BY holder_id, status ORDER BY end_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING), 2) AS premium_avg_3_prior,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY premium DESC, id), 3) AS premium_cume_dist,
        SIZE(COLLECT_SET(holder_id) OVER w_end_date_year) AS distinct_holder_id_in_end_date_year
    FROM recent
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status), w_holder_id_by_premium_ties AS (PARTITION BY holder_id ORDER BY premium DESC), w_end_date_year AS (PARTITION BY YEAR(end_date))
),
labeled AS (
    -- Add calculated columns
    SELECT
        *,
        LENGTH(TRIM(status)) AS status_len,
        CASE WHEN status IN ('EXPIRED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group
    FROM with_trends
    WHERE status IN ('LAPSED', 'ACTIVE')
)
-- Final projection
SELECT
    id,
    holder_id,
    end_date,
    status,
    holder_id_status_avg_premium,
    holder_id_status_max_premium,
    end_date_quartile,
    end_date_year_avg_premium,
    second_end_date,
    premium_share_of_end_date_year,
    premium_avg_7,
    distinct_status_in_end_date_year,
    premium_cume_dist,
    distinct_holder_id_in_end_date_year,
    status_group
FROM labeled
ORDER BY holder_id_status_max_premium DESC, status_group DESC
