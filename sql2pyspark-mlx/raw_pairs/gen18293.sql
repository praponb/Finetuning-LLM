select *, trim(status) as clean_status from payments where order_id = 39 and amount >= 830 order by amount;
