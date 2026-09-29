with ranked_shipments as (select *, dense_rank() over (partition by carrier order by delivered_date desc) as rnk from shipments) select * from ranked_shipments where rnk <= 3;
