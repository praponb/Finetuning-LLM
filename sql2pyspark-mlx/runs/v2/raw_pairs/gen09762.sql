select * from (select *, rank() over (partition by city order by sqft desc) as rnk from stores where region = 'North') as t where rnk <= 5 order by region
