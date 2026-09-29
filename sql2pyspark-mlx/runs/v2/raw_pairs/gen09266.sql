select year(opened_date) as opened_year, sum(sqft) as total_sqft, sum(sum(sqft)) over (order by year(opened_date)) as cumulative_sqft from stores group by year(opened_date)
