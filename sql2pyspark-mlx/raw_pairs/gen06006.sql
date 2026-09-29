WITH enriched AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY store_id ORDER BY units DESC NULLS FIRST) AS units_dense_rank,
        CORR(units, revenue) OVER (PARTITION BY YEAR(sale_date), store_id) AS corr_units_revenue,
        SUM(units) OVER w_ordered AS running_units
    FROM store_sales
    WHERE product_id IN (1, 7, 9)
    WINDOW w_ordered AS (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    s.id,
    s.store_id,
    s.product_id,
    s.units,
    s.revenue,
    s.seq_no,
    s.units_dense_rank,
    s.running_units,
    (SELECT SUM(t2.units) FROM store_sales t2 WHERE t2.product_id = s.product_id) AS sum_store_sale_units
FROM enriched s
ORDER BY seq_no
