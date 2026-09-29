SELECT id, opened_date, CASE WHEN city IN ('London', 'Tokyo') THEN 'Priority' ELSE 'Other' END AS city_group FROM stores WHERE region != 'West' OR region IN ('West', 'Central')
