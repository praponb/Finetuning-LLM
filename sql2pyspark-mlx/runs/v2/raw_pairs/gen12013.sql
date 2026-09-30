select city, length(country) as country_length, case when rating >= 3.8 then 'High' when rating >= 2.0 then 'Medium' else 'Low' end as rating_tier from suppliers order by 1 desc
