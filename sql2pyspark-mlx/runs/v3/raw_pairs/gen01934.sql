WITH store_totals AS (
    SELECT store_id, COUNT(DISTINCT quantity) AS unique_stores, COUNT(DISTINCT quantity) AS unique_products
    FROM inventory
    WHERE product_id IN (2, 3, 5)
    GROUP BY store_id
)
SELECT
    s.id,
    s.city,
    st.unique_stores,
    st.unique_products
FROM stores s
INNER JOIN store_totals st ON s.id = st.store_id
