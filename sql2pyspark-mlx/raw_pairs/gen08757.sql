SELECT SORT_ARRAY(COLLECT_SET(country)) AS country_list FROM suppliers WHERE rating < 2.9 AND supplier_name LIKE 'Si%';
