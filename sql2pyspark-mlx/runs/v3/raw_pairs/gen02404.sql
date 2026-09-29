select orders.id, orders.channel, orders.status from orders where orders.discount <= 0.39 and orders.store_id in (select id from stores where opened_date between '2024-03-01' and '2025-09-02')
