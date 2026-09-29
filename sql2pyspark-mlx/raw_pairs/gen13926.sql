select year(opened_date) as opened_year, sum(case when region = 'South' then sqft else 0 end) as south_sqft from stores group by 1
