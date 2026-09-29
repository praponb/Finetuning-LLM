SELECT c.name, s.end_date, ADD_MONTHS(s.start_date, 3) start_date_plus_months FROM customers c LEFT JOIN subscriptions s ON c.id = s.customer_id WHERE s.plan NOT IN ('premium', 'enterprise');
