select count(stock_qty) as stock_qty_count, count(*) as row_count from products where price >= 770 and supplier_id in (27, 28) order by stock_qty_count desc;
