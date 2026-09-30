SELECT country, started_at FROM sessions WHERE country NOT IN ('DE', 'JP', 'US') ORDER BY 1;
