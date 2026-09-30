SELECT id, signup_date, country, name, REGEXP_REPLACE(name, ' ', '_') AS name_slug FROM customers ORDER BY name_slug, id LIMIT 50;
