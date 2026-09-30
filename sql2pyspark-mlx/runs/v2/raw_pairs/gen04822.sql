select city, country, 'customer' as source from customers where age >= 45 union all select city, country, 'supplier' as source from suppliers where city in ('Tokyo', 'Singapore');
