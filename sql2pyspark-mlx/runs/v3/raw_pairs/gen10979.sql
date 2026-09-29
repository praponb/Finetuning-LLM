select id, product_id, unit_price, quantity from (select *, row_number() over (partition by product_id order by unit_price, id) as rnk from order_items) as r where rnk <= 5 order by product_id desc
