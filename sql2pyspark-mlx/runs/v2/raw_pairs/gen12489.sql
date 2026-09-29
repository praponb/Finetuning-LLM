SELECT id, opened_date, ROW_NUMBER() OVER (ORDER BY opened_date, id) rn
FROM stores
