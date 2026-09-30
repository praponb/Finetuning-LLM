select last_restock as event_date from inventory intersect select order_date as event_date from orders;
