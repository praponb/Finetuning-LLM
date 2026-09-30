select * from (select *, row_number() over (partition by order_id order by delivered_date desc, id) rnk from shipments where status in ('IN_TRANSIT', 'RETURNED', 'DELIVERED')) t where rnk <= 2;
