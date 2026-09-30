WITH deduped AS (
    SELECT *
    FROM store_sales
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date DESC, id) = 1
)
SELECT
    store_id,
    COUNT(*) AS cnt,
    BIT_AND(units) AS units_bit_and,
    COUNT(*) AS event_count
FROM deduped
GROUP BY store_id
ORDER BY cnt;
