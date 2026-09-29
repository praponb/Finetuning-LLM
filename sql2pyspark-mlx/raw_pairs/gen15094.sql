select id, status, paid_at, amount, lag(amount, 2) over (partition by status order by paid_at, id) as prev_amount from payments order by prev_amount desc;
