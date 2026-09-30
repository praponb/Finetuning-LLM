select a.opened_date, a.balance, a.status, c.name, c.age from accounts a left join customers c on c.id = a.customer_id where c.country in ('JP', 'US') and c.country in ('JP', 'US')
