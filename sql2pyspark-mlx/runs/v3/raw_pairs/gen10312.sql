select * from (select date_trunc('MONTH', paid_at) as paid_month, count(*) as total_count from payments group by date_trunc('MONTH', paid_at)) as sub where total_count >= 9;
