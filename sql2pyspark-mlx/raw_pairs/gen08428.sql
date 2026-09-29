select id, account_type, customer_id, datediff(current_date(), opened_date) / 365 as years_since_opened_date from accounts where customer_id = 33
