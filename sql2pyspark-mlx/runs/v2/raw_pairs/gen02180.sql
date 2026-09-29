select month(last_restock) as last_restock_month, count(quantity) as quantity_count, min(last_restock) as first_last_restock from inventory group by last_restock_month order by 3
