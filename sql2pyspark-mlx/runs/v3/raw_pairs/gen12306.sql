SELECT *, DATE_FORMAT(signup_date, 'MMM yyyy') AS signup_date_label, REGEXP_EXTRACT(email, '@(.+)$', 1) AS email_domain
FROM customers
