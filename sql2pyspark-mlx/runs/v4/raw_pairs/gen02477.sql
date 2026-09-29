select * from payments qualify row_number() over (partition by method order by paid_at desc nulls last, id) = 1;
