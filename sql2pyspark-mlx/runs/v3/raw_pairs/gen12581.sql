SELECT store_id, COALESCE(revenue, 0) revenue_filled, WEEKOFYEAR(sale_date) sale_date_weekofyear
FROM store_sales
WHERE revenue > 3400;
