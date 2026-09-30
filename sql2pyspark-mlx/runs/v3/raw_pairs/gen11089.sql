select s.id, s.status, c.is_active from subscriptions s left join customers c on s.customer_id = c.id where c.age is not null and s.monthly_fee >= 150 order by id desc, status desc limit 3
