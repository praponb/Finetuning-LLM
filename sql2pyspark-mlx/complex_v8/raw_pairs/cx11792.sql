WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status
    FROM policies
    WHERE holder_id >= 8
      AND start_date >= DATE '2024-10-22'
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MAX(premium) OVER w_holder_id_status AS holder_id_status_max_premium,
        ROW_NUMBER() OVER (ORDER BY premium ASC, id) AS premium_seq,
        NTH_VALUE(start_date, 2) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROUND(AVG(premium) OVER w_holder_id_status, 2) AS holder_id_status_avg_premium,
        ROUND(AVG(premium) OVER (ORDER BY premium ASC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS premium_avg_3,
        LEAD(start_date) OVER (ORDER BY start_date DESC, id) AS next_start_date,
        ROUND(premium / NULLIF(SUM(premium) OVER (PARTITION BY holder_id, status), 0), 4) AS premium_share_of_holder_id_status,
        MAX(premium) OVER (ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_premium_so_far,
        SUM(premium) OVER w_holder_id_status AS holder_id_status_sum_premium,
        ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY premium, id) AS premium_seq_2,
        ROW_NUMBER() OVER (PARTITION BY holder_id, status ORDER BY start_date, id) AS seq_no,
        DENSE_RANK() OVER (ORDER BY premium ASC) AS premium_dense_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY premium ASC, id), 3) AS premium_percent_rank,
        ROUND(CUME_DIST() OVER w_all_by_start_date_2, 3) AS start_date_cume_dist,
        LAST_VALUE(premium) OVER (ORDER BY premium ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_premium
    FROM valid
    WINDOW w_holder_id_status AS (PARTITION BY holder_id, status), w_all_by_start_date_2 AS (ORDER BY start_date, id)
),
first_rows AS (
    -- First row per group
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        status,
        holder_id_status_max_premium,
        second_start_date,
        holder_id_status_avg_premium,
        premium_avg_3,
        next_start_date,
        premium_share_of_holder_id_status,
        max_premium_so_far,
        holder_id_status_sum_premium,
        premium_seq_2,
        seq_no,
        premium_dense_rank,
        premium_percent_rank,
        start_date_cume_dist,
        last_premium
    FROM ranked
    WHERE premium_seq = 1
)
-- Final projection
SELECT
    id,
    holder_id,
    premium,
    start_date,
    status,
    holder_id_status_max_premium,
    second_start_date,
    holder_id_status_avg_premium,
    premium_avg_3,
    premium_share_of_holder_id_status,
    max_premium_so_far,
    premium_seq_2,
    seq_no,
    start_date_cume_dist,
    last_premium,
    FLOOR(premium_dense_rank / 2) * 2 AS premium_dense_rank_bucket,
    SUBSTRING(TRIM(status), 1, 3) AS status_prefix
FROM first_rows
ORDER BY max_premium_so_far DESC NULLS FIRST
