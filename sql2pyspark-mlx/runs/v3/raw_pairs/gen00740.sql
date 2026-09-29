select id, opened_date, ntile(4) over (order by opened_date, id) as quartile from stores where city = 'Singapore'
