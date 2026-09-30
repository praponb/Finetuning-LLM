SELECT id, region, DATE_ADD(opened_date, 1) AS opened_date_plus_1d, CASE WHEN region IN ('Central', 'North') THEN 'Tier 1' ELSE 'Other' END AS region_group FROM stores;
