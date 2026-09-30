select id, amount, ntile(3) over (order by amount desc, id) as amount_ntile from orders
