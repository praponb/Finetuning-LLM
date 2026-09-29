WITH filtered_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        status,
        CASE WHEN status IN ('LAPSED', 'CANCELLED') THEN 'group_a' WHEN status = 'EXPIRED' THEN 'group_b' ELSE 'other' END AS status_group
    FROM policies
    WHERE product IN ('AUTO', 'HEALTH')
      AND premium <= 3532.7
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        status_group,
        DENSE_RANK() OVER w_status_by_premium_ties AS premium_dense_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY status_group, status ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        COUNT(*) OVER w_status_group AS rows_in_status_group,
        premium - LAG(premium) OVER (PARTITION BY status ORDER BY premium DESC, id) AS premium_change,
        LEAD(start_date, 3) OVER w_all_by_premium AS next3_start_date,
        NTILE(3) OVER w_all_by_premium AS premium_tercile,
        NTH_VALUE(premium, 2) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status_group), 2) AS premium_vs_status_group_avg,
        DENSE_RANK() OVER (ORDER BY premium ASC) AS premium_dense_rank_2,
        LEAD(premium, 1, 0) OVER (ORDER BY premium ASC, id) AS next_premium,
        RANK() OVER (ORDER BY premium) AS premium_rank,
        LAST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY product ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_premium,
        SUM(premium) OVER w_status_group_holder_id AS status_group_holder_id_sum_premium,
        COUNT(*) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count
    FROM filtered_policys
    WINDOW w_status_by_premium_ties AS (PARTITION BY status ORDER BY premium DESC), w_status_group AS (PARTITION BY status_group), w_all_by_premium AS (ORDER BY premium ASC, id), w_status_group_holder_id AS (PARTITION BY status_group, holder_id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_holder_id_by_premium_change AS rn,
        LAG(last_known_premium, 3) OVER (ORDER BY rows_in_status_group, id) AS prev3_last_known_premium,
        NTH_VALUE(next3_start_date, 2) OVER (PARTITION BY holder_id ORDER BY premium_change, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_next3_start_date,
        COUNT(*) OVER (PARTITION BY YEAR(next3_start_date)) AS rows_in_next3_start_date_year,
        RANK() OVER (ORDER BY rows_in_status_group) AS rows_in_status_group_rank,
        COUNT(*) OVER (PARTITION BY premium_tercile, status ORDER BY premium_tercile, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_2,
        ROUND(AVG(running_count) OVER (PARTITION BY YEAR(next3_start_date)), 2) AS next3_start_date_year_avg_running_count
    FROM with_trends
    WINDOW w_holder_id_by_premium_change AS (PARTITION BY holder_id ORDER BY premium_change, id)
)
SELECT
    id,
    holder_id,
    premium,
    status,
    premium_dense_rank,
    premium_change,
    next3_start_date,
    premium_tercile,
    second_premium,
    premium_vs_status_group_avg,
    premium_dense_rank_2,
    next_premium,
    premium_rank,
    status_group_holder_id_sum_premium,
    rn,
    second_next3_start_date,
    rows_in_next3_start_date_year,
    rows_in_status_group_rank,
    running_count_2,
    next3_start_date_year_avg_running_count,
    LENGTH(TRIM(status_group)) AS status_group_len
FROM sequenced
ORDER BY premium_dense_rank_2 DESC
