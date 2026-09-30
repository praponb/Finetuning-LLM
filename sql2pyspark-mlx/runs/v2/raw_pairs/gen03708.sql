SELECT SORT_ARRAY(COLLECT_SET(method)) AS method_list, AVG(amount) AS avg_amount FROM payments WHERE amount IS NULL
