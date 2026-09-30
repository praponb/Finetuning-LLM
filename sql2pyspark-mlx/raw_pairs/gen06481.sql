SELECT country FROM suppliers WHERE country IN ('US', 'UK', 'JP') UNION ALL SELECT country FROM sessions WHERE customer_id IS NULL;
