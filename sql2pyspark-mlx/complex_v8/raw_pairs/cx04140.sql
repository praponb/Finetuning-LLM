WITH scoped AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status
    FROM policies
    WHERE holder_id <= 15
),
windowed AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(premium - AVG(premium) OVER (PARTITION BY status, product), 2) AS premium_vs_status_product_avg,
        MAX(premium) OVER (PARTITION BY product ORDER BY start_date DESC, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS premium_max_2_prior,
        ROW_NUMBER() OVER w_all_by_end_date AS end_date_seq,
        ROW_NUMBER() OVER (PARTITION BY status, MONTH(end_date) ORDER BY start_date, id) AS start_date_seq,
        RANK() OVER (ORDER BY end_date DESC) AS end_date_rank,
        RANK() OVER w_all_by_end_date AS end_date_rank_2,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY status, MONTH(end_date) ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY product ORDER BY start_date DESC, id)) AS days_since_prev_start_date,
        COUNT(*) OVER (PARTITION BY MONTH(start_date), holder_id) AS rows_in_start_date_month_holder_id,
        DENSE_RANK() OVER w_product_by_start_date AS start_date_dense_rank
    FROM scoped
    WINDOW w_all_by_end_date AS (ORDER BY end_date DESC, id), w_product_by_start_date AS (PARTITION BY product ORDER BY start_date DESC, id)
),
scored AS (
    -- Derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        end_date,
        status,
        premium_vs_status_product_avg,
        premium_max_2_prior,
        end_date_rank_2,
        second_start_date,
        rows_in_start_date_month_holder_id,
        start_date_dense_rank,
        LOWER(TRIM(product)) AS product_clean
    FROM windowed
    WHERE premium_max_2_prior > 4640.0
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    status,
    premium_vs_status_product_avg,
    premium_max_2_prior,
    end_date_rank_2,
    rows_in_start_date_month_holder_id,
    product_clean
FROM scored
ORDER BY product DESC
