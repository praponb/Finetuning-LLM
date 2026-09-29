select month(last_restock) as last_restock_month, round(stddev(quantity), 2) as stddev_quantity, count(quantity) as quantity_count from inventory group by 1;
