SELECT
    store_id,
    COUNT(DISTINCT store_id) AS distinct_store_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids_2,
    STDDEV_SAMP(units) AS units_stddev,
    CORR(revenue, units) AS corr_revenue_units,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT store_id) AS distinct_store_ids_3
FROM store_sales
GROUP BY CUBE (store_id)
