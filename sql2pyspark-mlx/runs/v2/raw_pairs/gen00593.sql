select c.city, count(distinct s.country) as unique_countrys from sessions s inner join customers c on s.customer_id = c.id where c.segment = 'SMB' group by c.city order by city limit 10;
