WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        LENGTH(TRIM(status)) AS status_len,
        ROUND(premium * 1.1, 2) AS premium_adj
    FROM policies
    WHERE start_date IS NOT NULL
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(status_len) OVER (PARTITION BY product) AS product_min_status_len,
        premium_adj - LAG(premium_adj) OVER (PARTITION BY status, holder_id ORDER BY status_len ASC, id) AS premium_adj_change,
        FIRST_VALUE(premium) IGNORE NULLS OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium,
        FIRST_VALUE(premium_adj) IGNORE NULLS OVER (PARTITION BY status, holder_id ORDER BY status_len ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_premium_adj,
        ROUND(AVG(status_len) OVER (PARTITION BY status_len), 2) AS status_len_avg_status_len,
        SIZE(COLLECT_SET(holder_id) OVER w_product) AS distinct_holder_id_in_product,
        LEAD(start_date) OVER (ORDER BY premium DESC, id) AS next_start_date,
        ROUND(PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id ORDER BY start_date DESC), 3) AS start_date_percent_rank,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', end_date), holder_id ORDER BY start_date DESC, id) AS start_date_tercile,
        ROW_NUMBER() OVER (ORDER BY premium DESC, id) AS premium_seq,
        NTH_VALUE(status_len, 2) OVER (ORDER BY premium DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_status_len,
        ROUND(CUME_DIST() OVER w_all_by_premium_ties, 3) AS premium_cume_dist,
        MAX(status_len) OVER (ORDER BY premium DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS status_len_max_3,
        COUNT(*) OVER (PARTITION BY product) AS rows_in_product
    FROM filtered
    WINDOW w_product AS (PARTITION BY product), w_all_by_premium_ties AS (ORDER BY premium DESC)
),
derived AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status_len,
        premium_adj,
        product_min_status_len,
        premium_adj_change,
        first_known_premium_adj,
        status_len_avg_status_len,
        distinct_holder_id_in_product,
        start_date_percent_rank,
        start_date_tercile,
        second_status_len,
        premium_cume_dist,
        status_len_max_3,
        rows_in_product,
        CASE WHEN status IN ('CANCELLED', 'LAPSED') THEN 'group_a' WHEN status = 'ACTIVE' THEN 'group_b' ELSE 'other' END AS status_group,
        premium_seq - premium AS premium_seq_minus_premium
    FROM sequenced
    WHERE premium_seq >= 4
)
SELECT
    id,
    product,
    premium,
    start_date,
    end_date,
    status_len,
    product_min_status_len,
    premium_adj_change,
    first_known_premium_adj,
    status_len_avg_status_len,
    distinct_holder_id_in_product,
    start_date_percent_rank,
    start_date_tercile,
    second_status_len,
    status_len_max_3,
    rows_in_product,
    status_group,
    ROUND(premium_adj * 0.25, 2) AS premium_adj_adj,
    ROUND(start_date_percent_rank / NULLIF(premium_cume_dist, 0), 3) AS start_date_percent_rank_per_premium_cume_dist
FROM derived
WHERE start_date < DATE '2025-02-14'
ORDER BY status_len, start_date_tercile
