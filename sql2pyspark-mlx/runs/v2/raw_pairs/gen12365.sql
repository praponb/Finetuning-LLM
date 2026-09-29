select c.segment, count(distinct s.device) as unique_devices
from sessions s
right join customers c on c.id = s.customer_id
where month(c.signup_date) in (1, 6, 7)
group by c.segment
having unique_devices >= 4
order by unique_devices, segment;
