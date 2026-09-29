select id, opened_date from accounts where opened_date = (select min(opened_date) from accounts);
