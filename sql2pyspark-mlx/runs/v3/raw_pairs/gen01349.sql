select c.city, max(a.balance) - min(a.balance) as balance_range, count(a.balance) as balance_count
from accounts a
join customers c on c.id = a.customer_id
where c.city = 'Chiang Mai'
group by c.city;
