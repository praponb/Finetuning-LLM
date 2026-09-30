SELECT *, LAST_DAY(opened_date) AS opened_date_month_end
FROM stores
WHERE region <> 'East' AND sqft <> 15238 AND city IN ('Tokyo', 'Chiang Mai')
ORDER BY opened_date_month_end
