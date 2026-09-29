select id, opened_date, status, balance from accounts where opened_date = (select min(opened_date) from accounts where customer_id is null) order by balance
