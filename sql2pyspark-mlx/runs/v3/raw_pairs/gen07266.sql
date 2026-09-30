select carrier, sum(cost) as total_cost, rank() over (order by sum(cost) desc) as rnk from shipments group by carrier order by total_cost, carrier limit 1
