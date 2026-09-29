select account_type, customer_id from accounts where customer_id = 16 intersect select account_type, customer_id from accounts where status = 'FROZEN';
