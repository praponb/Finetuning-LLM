select * from (select id, carrier, cost, dense_rank() over (partition by carrier order by cost desc) as rn from shipments) as r where rn <= 3;
