select s.*, x.order_date
from stores s
left join lateral (
        select c.order_date
        from orders c
        where c.store_id = s.id
        order by c.shipping_fee desc, c.id desc
        limit 1
    ) x on true;
