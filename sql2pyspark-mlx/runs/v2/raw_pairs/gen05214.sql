select s.sqft, t.total_amount, t.mobile_discount
from stores s
inner join (select store_id, sum(amount) total_amount, sum(case when channel = 'mobile' then discount else 0 end) mobile_discount
    from orders
    group by store_id) t on s.id = t.store_id
order by s.sqft, mobile_discount
