select
    s.id,
    s.store_id,
    s.order_date,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    top1.payment_status,
    top1.payment_paid_at
from orders s
left join lateral (
        select c.status as payment_status, c.paid_at as payment_paid_at
        from payments c
        where c.order_id = s.id
          and c.paid_at <= s.order_date
        order by c.paid_at desc, c.id desc
        limit 1
    ) top1 on true;
