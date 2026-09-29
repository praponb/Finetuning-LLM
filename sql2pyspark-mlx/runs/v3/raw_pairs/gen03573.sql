select *, lower(email) as email_lower from customers where is_active order by country desc;
