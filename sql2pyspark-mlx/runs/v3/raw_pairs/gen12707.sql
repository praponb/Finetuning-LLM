select country from customers union all select country from sessions where country in ('US', 'SG', 'TH') order by country
