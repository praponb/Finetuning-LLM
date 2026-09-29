with RankedStores as (select *, rank() over (partition by region order by sqft desc) as rnk from stores) select * from RankedStores where rnk <= 3;
