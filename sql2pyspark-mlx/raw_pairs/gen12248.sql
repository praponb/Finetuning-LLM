SELECT
    suppliers.rating,
    suppliers.city,
    products.category,
    SORT_ARRAY(products.tags) AS sorted_tags
FROM suppliers
LEFT OUTER JOIN products ON suppliers.id = products.supplier_id
