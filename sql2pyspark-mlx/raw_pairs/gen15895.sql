select city, sum(case when region = 'West' then sqft else 0 end) west_sqft from stores where opened_date <= '2025-07-01' group by 1 having west_sqft >= 31;
