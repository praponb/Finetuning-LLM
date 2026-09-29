select c.email, c.is_active, c.credit_limit, s.browser, s.device from customers c left join sessions s on c.id = s.customer_id where s.customer_id in (7, 15, 40)
