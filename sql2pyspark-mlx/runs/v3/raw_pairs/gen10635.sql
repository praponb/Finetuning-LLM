select status, round(avg(amount), 2) avg_amount from payments group by 1;
