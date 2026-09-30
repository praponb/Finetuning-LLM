WITH filtered AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date
    FROM policies
    WHERE holder_id >= 19
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        DATEDIFF(start_date, LAG(start_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id)) AS days_since_prev_start_date,
        FIRST_VALUE(end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date,
        RANK() OVER (ORDER BY start_date) AS start_date_rank,
        NTILE(3) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id) AS end_date_tercile,
        LAG(end_date) OVER w_all_by_start_date AS prev_end_date,
        NTH_VALUE(start_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_start_date,
        ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id) AS end_date_seq,
        DENSE_RANK() OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id) AS end_date_dense_rank,
        ROW_NUMBER() OVER (ORDER BY start_date, id) AS start_date_seq,
        LAG(start_date, 3) OVER w_start_date_month_end_date_mo AS prev3_start_date,
        DATEDIFF(end_date, LAG(end_date) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id)) AS days_since_prev_end_date,
        NTH_VALUE(end_date, 2) OVER (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_end_date
    FROM filtered
    WINDOW w_all_by_start_date AS (ORDER BY start_date, id), w_start_date_month_end_date_mo AS (PARTITION BY DATE_TRUNC('MONTH', start_date), MONTH(end_date) ORDER BY end_date DESC, id)
)
-- Final projection
SELECT
    id,
    holder_id,
    product,
    end_date,
    first_end_date,
    start_date_rank,
    end_date_tercile,
    prev_end_date,
    second_start_date,
    start_date_seq,
    prev3_start_date,
    second_end_date
FROM sequenced
ORDER BY start_date_seq, prev_end_date DESC
