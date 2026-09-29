WITH scoped AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status
    FROM policies
    WHERE product = 'AUTO'
      AND YEAR(start_date) = 2025
),
with_trends AS (
    -- Window metrics over several partitions
    SELECT
        *,
        NTH_VALUE(start_date, 2) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        LAST_VALUE(start_date) IGNORE NULLS OVER (PARTITION BY product, holder_id ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_start_date,
        LAST_VALUE(start_date) OVER (PARTITION BY product, holder_id ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_start_date,
        FIRST_VALUE(start_date) OVER (PARTITION BY product, holder_id ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_start_date,
        DENSE_RANK() OVER w_all_by_start_date_ties AS start_date_dense_rank,
        ROUND(CUME_DIST() OVER w_product_holder_id_by_start_d_3, 3) AS start_date_cume_dist,
        DATEDIFF(start_date, LAG(start_date) OVER w_all_by_end_date) AS days_since_prev_start_date,
        ROUND(PERCENT_RANK() OVER (ORDER BY start_date DESC, id), 3) AS start_date_percent_rank
    FROM scoped
    WINDOW w_all_by_start_date_ties AS (ORDER BY start_date DESC), w_product_holder_id_by_start_d_3 AS (PARTITION BY product, holder_id ORDER BY start_date ASC), w_all_by_end_date AS (ORDER BY end_date DESC, id)
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        id,
        holder_id,
        end_date,
        status,
        first_start_date,
        start_date_cume_dist,
        start_date_percent_rank,
        MIN(start_date_percent_rank) OVER (ORDER BY first_start_date DESC NULLS FIRST, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS min_start_date_percent_rank_so_far,
        ROUND(start_date_dense_rank - AVG(start_date_dense_rank) OVER w_last_known_start_date_year_h, 2) AS start_date_dense_rank_vs_last_known_start_date_y,
        MAX(start_date_dense_rank) OVER (PARTITION BY product ORDER BY days_since_prev_start_date, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS start_date_dense_rank_max_3,
        ROUND(CUME_DIST() OVER (PARTITION BY product, MONTH(start_date) ORDER BY start_date_dense_rank DESC, id), 3) AS start_date_dense_rank_cume_dist,
        MAX(start_date_percent_rank) OVER (PARTITION BY product ORDER BY days_since_prev_start_date, id ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS start_date_percent_rank_max_1_prior,
        ROUND(AVG(start_date_percent_rank) OVER (PARTITION BY YEAR(last_known_start_date), holder_id), 2) AS last_known_start_date_year_holder_id_avg_start_d,
        MAX(start_date_percent_rank) OVER (ORDER BY first_start_date DESC NULLS FIRST, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS start_date_percent_rank_max_7,
        NTILE(10) OVER (ORDER BY first_start_date DESC NULLS FIRST, id) AS first_start_date_decile
    FROM with_trends
    WINDOW w_last_known_start_date_year_h AS (PARTITION BY YEAR(last_known_start_date), holder_id)
)
-- Final projection
SELECT
    id,
    end_date,
    status,
    start_date_cume_dist,
    start_date_percent_rank,
    min_start_date_percent_rank_so_far,
    start_date_dense_rank_max_3,
    start_date_dense_rank_cume_dist,
    start_date_percent_rank_max_1_prior,
    last_known_start_date_year_holder_id_avg_start_d,
    first_start_date_decile,
    ROUND(start_date_percent_rank / NULLIF(first_start_date_decile, 0), 3) AS start_date_percent_rank_per_first_start_date_dec
FROM sequenced
WHERE min_start_date_percent_rank_so_far >= 3.0
ORDER BY id
