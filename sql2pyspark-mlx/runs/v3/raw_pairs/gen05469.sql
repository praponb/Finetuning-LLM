SELECT
    id,
    supplier_name,
    rating,
    COALESCE(rating, 0) AS rating_filled
FROM suppliers
