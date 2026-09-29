SELECT
    id,
    city,
    rating,
    supplier_name,
    LAST_VALUE(supplier_name) OVER (PARTITION BY city ORDER BY rating DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_supplier_name
FROM suppliers
WHERE country IN ('US', 'DE', 'TH')
ORDER BY last_supplier_name ASC
