SELECT credit_limit, CASE WHEN is_active THEN 'Yes' ELSE 'No' END AS is_active_label, LOWER(country) AS country_lower FROM customers WHERE country = 'US' AND city = 'Berlin';
