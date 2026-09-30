select id, shipped_date, rank() over (order by shipped_date) as rnk from shipments
