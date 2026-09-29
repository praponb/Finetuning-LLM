select signup_date, city from customers where segment not in ('Corporate', 'SMB', 'Consumer') union select signup_date, city from customers where is_active = true;
