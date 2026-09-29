SELECT
    brand,
    ROUND(STDDEV(price), 2) AS stddev_price,
    SORT_ARRAY(COLLECT_SET(brand)) AS brand_list,
    SUM(stock_qty) AS total_stock_qty
FROM products
WHERE ARRAY_CONTAINS(tags, 'sale') OR brand != 'Stark'
GROUP BY brand;
