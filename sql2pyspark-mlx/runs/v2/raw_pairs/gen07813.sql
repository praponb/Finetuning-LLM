select id, status, paid_at, method, to_date(paid_at) as paid_at_date, upper(method) as method_upper from payments where hour(paid_at) between 8 and 12;
