select case when cost >= 76 then 'high' when cost >= 44 then 'mid' else 'low' end as cost_bucket, order_id, max(cost) - min(cost) as cost_range from shipments group by 1, 2
