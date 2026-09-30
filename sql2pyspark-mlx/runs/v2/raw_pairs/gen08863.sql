select s.id, s.plan, s.customer_id, s.monthly_fee, c.email, c.credit_limit from subscriptions s inner join customers c on s.customer_id = c.id where c.country in ('TH', 'SG') order by 6 desc;
