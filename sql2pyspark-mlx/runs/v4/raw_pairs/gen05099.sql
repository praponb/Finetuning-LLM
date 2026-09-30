select * from (select quarter(shipped_date) as shipped_quarter, status, cost from shipments)
pivot (count(cost) for status in ('DELIVERED', 'LOST', 'IN_TRANSIT'))
order by shipped_quarter
