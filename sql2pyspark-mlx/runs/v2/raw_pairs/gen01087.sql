select id, region, store_name, opened_date, case when sqft >= 14007 then 'High' when sqft >= 4639 then 'Medium' else 'Low' end as sqft_tier from stores where opened_date > '2024-09-01';
