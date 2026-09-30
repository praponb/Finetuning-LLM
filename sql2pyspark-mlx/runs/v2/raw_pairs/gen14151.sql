with top_shipments as (select *, row_number() over (partition by carrier order by delivered_date desc, id) rnk from shipments) select id, carrier, delivered_date from top_shipments where rnk <= 2;
