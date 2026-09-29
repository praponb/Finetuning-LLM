select id, status, paid_at, amount, lead(amount, 1) over (partition by status order by paid_at, id) next_amount, ntile(10) over (partition by status order by paid_at, id) quartile from payments;
