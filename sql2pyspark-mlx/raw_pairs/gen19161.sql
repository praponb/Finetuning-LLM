select status, max(opened_date) as latest_opened_date from accounts where customer_id is not null group by status order by status;
