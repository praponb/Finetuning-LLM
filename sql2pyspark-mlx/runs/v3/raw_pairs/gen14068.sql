select status, method, sum(case when method = 'promptpay' then amount else 0 end) as promptpay_amount, count(amount) as amount_count from payments group by status, method order by method asc;
