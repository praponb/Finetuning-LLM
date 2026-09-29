with Numbered as (select *, row_number() over (partition by status order by paid_at desc, id) as rn from payments) select id, status, paid_at, method from Numbered where rn <= 5 order by method desc
