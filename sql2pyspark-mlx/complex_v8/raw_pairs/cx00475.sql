WITH base AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        end_date,
        status
    FROM policies
    WHERE start_date >= DATE '2024-11-20'
      AND status = 'EXPIRED'
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY product ORDER BY end_date, id) AS end_date_decile,
        DENSE_RANK() OVER w_end_date_month_by_end_date AS end_date_dense_rank,
        DENSE_RANK() OVER (ORDER BY end_date) AS end_date_dense_rank_2,
        ROW_NUMBER() OVER (ORDER BY end_date, id) AS seq_no,
        FIRST_VALUE(end_date) OVER (PARTITION BY product ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        ROW_NUMBER() OVER w_end_date_month_by_end_date AS rn,
        RANK() OVER (PARTITION BY holder_id ORDER BY end_date ASC NULLS LAST) AS end_date_rank,
        LAST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY end_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_end_date
    FROM base
    WINDOW w_end_date_month_by_end_date AS (PARTITION BY MONTH(end_date) ORDER BY end_date, id)
),
labeled AS (
    -- Label and score rows
    SELECT
        *,
        UPPER(TRIM(product)) AS product_clean,
        ROUND(seq_no / NULLIF(end_date_decile, 0), 3) AS seq_no_per_end_date_decile,
        LOWER(REGEXP_REPLACE(TRIM(status), ' ', '_')) AS status_slug
    FROM metrics
)
SELECT
    id,
    holder_id,
    status,
    end_date_decile,
    end_date_dense_rank,
    end_date_dense_rank_2,
    seq_no,
    rn,
    last_known_end_date,
    product_clean,
    seq_no_per_end_date_decile,
    status_slug,
    ABS(end_date_rank - 7) AS end_date_rank_distance
FROM labeled
ORDER BY end_date_dense_rank NULLS LAST, end_date_rank_distance NULLS LAST
