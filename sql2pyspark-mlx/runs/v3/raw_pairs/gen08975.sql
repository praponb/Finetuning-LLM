select opened_date from stores intersect select sale_date as opened_date from store_sales where store_id in (4, 5, 6) order by opened_date;
