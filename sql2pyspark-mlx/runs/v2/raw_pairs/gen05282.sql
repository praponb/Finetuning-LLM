SELECT *, CASE WHEN country IN ('DE', 'TH') THEN 'Tier 1' ELSE 'Other' END AS country_group FROM sessions ORDER BY device;
