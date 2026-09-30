WITH valid AS (
    SELECT
        id,
        supplier_name,
        country,
        rating,
        ROUND(rating * 1.07, 2) AS rating_adjusted,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS DISTINCT FROM 'London'
)
SELECT
    *,
    ROW_NUMBER() OVER w_ordered AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS seq_no,
    ROW_NUMBER() OVER w_part AS event_seq,
    LAST_VALUE(rating_adjusted, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_rating_adjusted
FROM valid
WHERE country = 'JP'
WINDOW w_ordered AS (PARTITION BY country ORDER BY rating, id), w_part AS (ORDER BY rating_adjusted DESC, id);
