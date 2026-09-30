select amount, order_id, paid_at, case when status = 'REFUNDED' then 1 else 0 end as is_refunded, round(amount, 0) as amount_rounded from payments where status = 'FAILED' order by amount_rounded
