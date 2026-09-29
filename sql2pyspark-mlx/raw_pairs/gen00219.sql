select id, method, amount from (select *, row_number() over (partition by method order by amount desc nulls last, id) as latest_rank from payments) x where latest_rank = 1
