SELECT country FROM suppliers WHERE country IN ('SG', 'JP') UNION ALL SELECT country FROM sessions WHERE device NOT IN ('mobile', 'desktop', 'tablet') ORDER BY country DESC
