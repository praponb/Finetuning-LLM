select status, min(amount) as min_amount, count(amount) as amount_count from payments where status = 'REFUNDED' and amount between 460 and 780 and order_id in (3, 12, 21) group by 1;
