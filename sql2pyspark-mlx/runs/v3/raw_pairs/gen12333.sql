SELECT * FROM sessions WHERE country NOT IN ('DE', 'JP', 'TH') AND browser IN ('Edge', 'Chrome') ORDER BY customer_id DESC;
