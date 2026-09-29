SELECT
    *,
    CUME_DIST() OVER (ORDER BY units NULLS LAST) AS units_cume_dist,
    AVG(revenue) OVER w_running AS revenue_avg_6
FROM store_sales
WINDOW w_running AS (PARTITION BY MONTH(sale_date), product_id ORDER BY revenue, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW);
