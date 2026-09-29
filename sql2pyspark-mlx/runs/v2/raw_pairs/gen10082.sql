select method, status, count(amount) as amount_count, min(amount) as min_amount from payments group by method, status;
