select * from (select segment, city, credit_limit from customers)
pivot (max(credit_limit) for city in ('Singapore', 'New York', 'Berlin', 'Chiang Mai'))
