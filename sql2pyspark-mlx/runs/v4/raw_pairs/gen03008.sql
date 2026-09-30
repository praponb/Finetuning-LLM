WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT *, DATE_TRUNC('WEEK', sale_date) AS sale_date_period
    FROM store_sales
    WHERE revenue > 2700
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.opened_date,
        p2.sqft,
        p2.city
    FROM filtered s
    INNER JOIN stores p2
        ON s.store_id = p2.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_recent AS event_seq,
        STDDEV_SAMP(revenue) OVER (PARTITION BY store_id) AS store_id_stddev_samp_revenue
    FROM with_dims
    WINDOW w_recent AS (PARTITION BY city ORDER BY sale_date DESC, id)
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sale_date DESC, id) = 1
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    units,
    opened_date,
    sqft,
    store_id_stddev_samp_revenue
FROM deduped
ORDER BY id DESC
LIMIT 10
