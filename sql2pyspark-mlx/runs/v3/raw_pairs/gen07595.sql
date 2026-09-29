SELECT id, device, country FROM sessions WHERE device NOT IN ('desktop', 'tablet', 'mobile') UNION ALL SELECT id, device, country FROM sessions WHERE country NOT IN ('TH', 'JP')
