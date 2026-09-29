select c.city, c.is_active, c.name, s.browser, s.country, s.device from customers c join sessions s on s.customer_id = c.id where s.device in ('mobile', 'desktop');
