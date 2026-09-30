select * from (select id, region, opened_date, row_number() over (partition by region order by opened_date desc, id) as pos from stores) as r where pos <= 2
