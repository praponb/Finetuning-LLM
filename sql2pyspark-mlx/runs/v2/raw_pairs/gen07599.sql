select
    date_format(s.opened_date, 'yyyy-MM') as opened_month,
    s.city,
    round(stddev(p.amount), 2) as stddev_amount,
    count(distinct p.amount) as unique_methods
from payments p
left join orders o on p.order_id = o.id
inner join stores s on o.store_id = s.id
where p.method != 'cash'
group by date_format(s.opened_date, 'yyyy-MM'), s.city
order by unique_methods desc
