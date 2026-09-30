SELECT
    id,
    category,
    launch_date,
    cost,
    ARRAY_JOIN(tags, ', ') AS tags_text,
    COALESCE(brand, 'N/A') AS brand_filled
FROM products
WHERE supplier_id IN (7, 11, 33) AND (brand IN ('Initech', 'Umbrella') OR brand IN ('Umbrella', 'Stark', 'Initech'))
