select sum(case when region = 'Central' then sqft else 0 end) as central_sqft from stores;
