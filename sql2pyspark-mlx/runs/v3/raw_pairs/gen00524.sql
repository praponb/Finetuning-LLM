select c.segment, sort_array(collect_set(t.merchant)) as merchant_list, round(avg(t.amount), 2) as avg_amount
from transactions t
left join accounts a on t.account_id = a.id
left join customers c on c.id = a.customer_id
where year(a.opened_date) >= 2023
group by c.segment
having avg_amount > 51
order by avg_amount desc, c.segment
