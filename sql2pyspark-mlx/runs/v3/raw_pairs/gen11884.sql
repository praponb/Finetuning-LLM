SELECT method, status, UPPER(method) method_upper, INITCAP(TRIM(method)) method_title FROM payments WHERE amount >= 660 ORDER BY method DESC;
