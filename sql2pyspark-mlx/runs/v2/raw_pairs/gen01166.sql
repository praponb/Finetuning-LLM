select *, datediff(current_date(), last_restock) as days_since_last_restock from inventory where product_id in (4, 9, 10);
