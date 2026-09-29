WITH valid_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status,
        ABS(premium - 2289.0) AS premium_distance,
        DATE_TRUNC('MONTH', start_date) AS start_date_month,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE status IN ('CANCELLED', 'LAPSED', 'EXPIRED')
      AND start_date BETWEEN DATE '2024-03-15' AND DATE '2025-05-02'
),
with_windows AS (
    -- Previous / next values per key
    SELECT
        id,
        product,
        premium,
        start_date,
        end_date,
        status,
        premium_distance,
        start_date_month,
        DENSE_RANK() OVER w_all_by_start_date AS start_date_dense_rank,
        SIZE(COLLECT_SET(status) OVER (PARTITION BY holder_id, product)) AS distinct_status_in_holder_id_product,
        NTH_VALUE(premium_filled, 2) OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY start_date_month DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_filled,
        DENSE_RANK() OVER (PARTITION BY holder_id, DATE_TRUNC('MONTH', end_date) ORDER BY start_date_month DESC) AS start_date_month_dense_rank
    FROM valid_policys
    WINDOW w_all_by_start_date AS (ORDER BY start_date DESC NULLS FIRST, id)
),
scored AS (
    -- Add calculated columns
    SELECT *, LAST_DAY(start_date) AS start_date_month_end
    FROM with_windows
    WHERE start_date_dense_rank <= 6
)
-- Native SQL UNPIVOT (wide to long)
SELECT id, field, reading
FROM (
    SELECT id, start_date_dense_rank, start_date_month_dense_rank
    FROM scored
) src
UNPIVOT (
    reading FOR field IN (start_date_dense_rank, start_date_month_dense_rank)
) unp
