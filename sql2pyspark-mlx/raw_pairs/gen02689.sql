select email, country, is_active from customers where month(signup_date) in (3, 7, 12)
