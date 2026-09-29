SELECT
    *,
    CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units,
    AVG(revenue) OVER (PARTITION BY product_id ORDER BY units, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS revenue_avg_5,
    SUM(units) OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_units,
    SUM(units) OVER w_hist AS running_units
FROM store_sales
WINDOW w_hist AS (PARTITION BY store_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
