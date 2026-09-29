select opened_date, 'store' as source from stores where opened_date is not null union select order_date, 'order' as source from orders where order_date is not null order by opened_date desc
