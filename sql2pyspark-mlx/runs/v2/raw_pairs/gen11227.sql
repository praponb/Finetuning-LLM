SELECT *, CASE WHEN segment IN ('SMB', 'Corporate') THEN 'Core' ELSE 'Other' END AS segment_group FROM customers
