select id, status, paid_at, method from (select id, status, paid_at, method, row_number() over (partition by status order by paid_at desc, id) as rnk from payments) as r where rnk = 1 order by method
