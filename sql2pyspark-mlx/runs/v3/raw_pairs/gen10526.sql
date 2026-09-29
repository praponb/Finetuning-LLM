SELECT
    id,
    opened_date,
    DATE_FORMAT(opened_date, 'MMM yyyy') AS opened_date_label,
    DATEDIFF(CURRENT_DATE(), opened_date) AS days_since_opened_date
FROM stores;
