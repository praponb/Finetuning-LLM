WITH FilteredStores AS (
    SELECT *
    FROM stores
    WHERE UPPER(store_name) LIKE '%LD %'
),
summary AS (
    SELECT MONTH(opened_date) AS opened_month, MAX(opened_date) AS last_opened_date
    FROM FilteredStores
    GROUP BY MONTH(opened_date)
)
SELECT opened_month, last_opened_date
FROM summary;
