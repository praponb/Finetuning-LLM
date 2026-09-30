WITH valid AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        premium,
        start_date,
        end_date,
        COALESCE(product, 'UNKNOWN') AS product_or_unknown,
        SUBSTRING(TRIM(product), 1, 4) AS product_prefix
    FROM policies
    WHERE holder_id IS NOT NULL
      AND status IS NOT NULL
),
ranked AS (
    -- Previous / next values per key
    SELECT
        *,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY holder_id ORDER BY start_date ASC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date,
        RANK() OVER w_start_date_year_holder_id_by AS end_date_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY end_date, id), 3) AS end_date_percent_rank,
        LAG(end_date, 3) OVER (ORDER BY start_date DESC, id) AS prev3_end_date,
        SUM(premium) OVER (PARTITION BY holder_id ORDER BY start_date ASC, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS premium_sum_7,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY MONTH(end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date_2,
        LAG(start_date) OVER (PARTITION BY MONTH(end_date) ORDER BY end_date DESC, id) AS prev_start_date,
        ROW_NUMBER() OVER w_holder_id_by_start_date AS seq_no,
        ROUND(premium - AVG(premium) OVER (PARTITION BY holder_id), 2) AS premium_vs_holder_id_avg,
        ROUND(CUME_DIST() OVER w_start_date_year_holder_id_by, 3) AS end_date_cume_dist,
        ROUND(PERCENT_RANK() OVER (ORDER BY end_date), 3) AS end_date_percent_rank_2,
        MAX(premium) OVER (PARTITION BY YEAR(start_date), holder_id ORDER BY end_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS premium_max_3,
        ROUND(premium - AVG(premium) OVER (PARTITION BY product_or_unknown), 2) AS premium_vs_product_or_unknown_avg,
        ROW_NUMBER() OVER (PARTITION BY YEAR(start_date), holder_id ORDER BY end_date DESC, id) AS seq_no_2,
        ROUND(CUME_DIST() OVER (PARTITION BY holder_id ORDER BY start_date ASC, id), 3) AS start_date_cume_dist
    FROM valid
    WINDOW w_start_date_year_holder_id_by AS (PARTITION BY YEAR(start_date), holder_id ORDER BY end_date DESC, id), w_holder_id_by_start_date AS (PARTITION BY holder_id ORDER BY start_date ASC, id)
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(*) OVER (PARTITION BY YEAR(end_date)) AS rows_in_end_date_year,
        ROUND(PERCENT_RANK() OVER w_end_date_rank_second_end_dat, 3) AS seq_no_2_percent_rank,
        DENSE_RANK() OVER (PARTITION BY MONTH(prev_start_date), YEAR(start_date) ORDER BY end_date) AS end_date_dense_rank,
        ROUND(end_date_percent_rank - AVG(end_date_percent_rank) OVER (PARTITION BY YEAR(end_date)), 2) AS end_date_percent_rank_vs_end_date_year_avg,
        SUM(seq_no) OVER (PARTITION BY end_date_rank ORDER BY second_end_date DESC, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS seq_no_sum_3
    FROM ranked
    WINDOW w_end_date_year AS (PARTITION BY YEAR(end_date)), w_end_date_rank_second_end_dat AS (PARTITION BY end_date_rank, YEAR(second_end_date) ORDER BY seq_no_2, id)
)
SELECT
    id,
    holder_id,
    premium,
    start_date,
    end_date,
    product_or_unknown,
    second_end_date,
    end_date_rank,
    end_date_percent_rank,
    prev3_end_date,
    premium_sum_7,
    prev_start_date,
    seq_no,
    end_date_percent_rank_2,
    premium_max_3,
    seq_no_2,
    start_date_cume_dist,
    rows_in_end_date_year,
    seq_no_2_percent_rank,
    end_date_dense_rank,
    end_date_percent_rank_vs_end_date_year_avg
FROM sequenced
ORDER BY seq_no_2, id
LIMIT 20
