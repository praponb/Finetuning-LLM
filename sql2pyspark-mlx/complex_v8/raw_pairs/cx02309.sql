WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status,
        CONCAT_WS(' / ', product, status) AS product_status_label
    FROM policies
    WHERE holder_id IS NOT NULL
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        id,
        holder_id,
        product,
        start_date,
        product_status_label,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY product ORDER BY end_date DESC, id)) AS days_since_prev_end_date,
        ROUND(CUME_DIST() OVER (PARTITION BY product, status ORDER BY start_date DESC), 3) AS start_date_cume_dist,
        DENSE_RANK() OVER w_product_by_end_date_ties AS end_date_dense_rank,
        NTILE(4) OVER (PARTITION BY status ORDER BY start_date, id) AS start_date_quartile,
        LAST_VALUE(start_date) OVER (PARTITION BY product, status ORDER BY start_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        FIRST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_start_date,
        NTILE(3) OVER w_product_status_by_start_date_3 AS start_date_tercile,
        ROUND(PERCENT_RANK() OVER (PARTITION BY product, status ORDER BY start_date DESC, id), 3) AS start_date_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY product ORDER BY end_date DESC, id) AS end_date_seq,
        LEAD(end_date) OVER (PARTITION BY status ORDER BY start_date, id) AS next_end_date,
        RANK() OVER (PARTITION BY product, status ORDER BY start_date DESC) AS start_date_rank
    FROM cleaned_policys
    WINDOW w_product_by_end_date_ties AS (PARTITION BY product ORDER BY end_date DESC), w_product_status_by_start_date AS (PARTITION BY product, status ORDER BY start_date DESC), w_product_status_by_start_date_3 AS (PARTITION BY product, status ORDER BY start_date DESC, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_all_by_start_date_percent_ra AS rn,
        SIZE(COLLECT_SET(product) OVER w_start_date_rank_end_date_seq) AS distinct_product_in_start_date_rank_end_date_seq,
        SIZE(COLLECT_SET(holder_id) OVER (PARTITION BY start_date_rank, end_date_seq)) AS distinct_holder_id_in_start_date_rank_end_date_s
    FROM with_trends
    WINDOW w_all_by_start_date_percent_ra AS (ORDER BY start_date_percent_rank DESC, id), w_start_date_rank_end_date_seq AS (PARTITION BY start_date_rank, end_date_seq)
)
SELECT
    id,
    product,
    start_date,
    product_status_label,
    days_since_prev_end_date,
    start_date_cume_dist,
    end_date_dense_rank,
    start_date_quartile,
    first_known_start_date,
    start_date_tercile,
    start_date_percent_rank,
    next_end_date,
    start_date_rank,
    rn,
    distinct_product_in_start_date_rank_end_date_seq,
    distinct_holder_id_in_start_date_rank_end_date_s,
    CASE WHEN product IN ('HEALTH', 'TRAVEL') THEN 'group_a' WHEN product = 'HOME' THEN 'group_b' ELSE 'other' END AS product_group
FROM sequenced
WHERE rn <= 9
ORDER BY distinct_product_in_start_date_rank_end_date_seq DESC, days_since_prev_end_date DESC
