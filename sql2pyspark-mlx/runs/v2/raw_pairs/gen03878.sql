select id, signup_date, credit_limit, email, cast(is_active as int) as is_active_flag, initcap(trim(email)) as email_title from customers where is_active
