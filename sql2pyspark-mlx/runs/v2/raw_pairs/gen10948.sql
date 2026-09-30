SELECT
    id,
    product_id,
    units,
    PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY units) AS units_pct_rank
FROM store_sales
