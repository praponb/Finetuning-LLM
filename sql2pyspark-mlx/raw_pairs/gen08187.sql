WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        units,
        DATEDIFF(CURRENT_DATE(), sale_date) AS days_since_sale_date,
        CASE WHEN units >= 34 THEN 'High' WHEN units >= 17 THEN 'Medium' ELSE 'Low' END AS units_band
    FROM store_sales
    WHERE revenue <= 3500
)
-- Window metrics over several different partitions
SELECT
    *,
    STDDEV_SAMP(units) OVER (PARTITION BY store_id) AS store_id_stddev_samp_units,
    PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY units NULLS LAST) AS units_percent_rank,
    ROW_NUMBER() OVER (ORDER BY days_since_sale_date DESC, id) AS row_seq,
    AVG(days_since_sale_date) OVER w_part AS days_since_sale_date_avg_5
FROM valid
WINDOW w_part AS (PARTITION BY store_id ORDER BY units, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW);
