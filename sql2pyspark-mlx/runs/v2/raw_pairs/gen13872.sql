select id, status, shipped_date, rank() over (partition by status order by shipped_date) as shipped_date_rank from shipments;
