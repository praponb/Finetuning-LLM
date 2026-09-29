SELECT
    id,
    store_name,
    city,
    region,
    opened_date,
    sqft,
    LAG(sqft) OVER w_ordered AS prev_sqft,
    LEAD(sqft) OVER w_ordered AS next_sqft,
    ROW_NUMBER() OVER w_ordered AS event_seq
FROM stores
WINDOW w_ordered AS (PARTITION BY city ORDER BY sqft, id);
