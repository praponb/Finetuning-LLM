SELECT id, city, DATE_ADD(signup_date, 75) signup_date_plus_75d FROM customers WHERE is_active = FALSE;
