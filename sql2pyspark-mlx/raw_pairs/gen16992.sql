select customer_id, count(*) account_count from accounts where opened_date > '2023-10-12' and status = 'FROZEN' and customer_id is not null group by customer_id;
