with ranked as (select *, row_number() over (partition by status order by paid_at desc, id) pos from payments) select id, status, paid_at from ranked where pos <= 2;
