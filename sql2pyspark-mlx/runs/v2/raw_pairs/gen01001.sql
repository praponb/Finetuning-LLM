select c.signup_date, c.is_active, c.city, a.account_type, a.opened_date, lower(c.segment) as segment_lower from customers c left outer join accounts a on a.customer_id = c.id
