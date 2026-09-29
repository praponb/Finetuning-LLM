WITH with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.cost
    FROM suppliers s
    LEFT JOIN LATERAL (
            SELECT c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        rating,
        LEAD(rating, 2) OVER (PARTITION BY country ORDER BY rating, id) AS next_rating,
        NTILE(4) OVER w_running AS rating_bucket,
        STDDEV_SAMP(rating) OVER w_running_2 AS city_stddev_samp_rating
    FROM with_last
    WHERE country IN ('SG', 'JP')
    WINDOW w_running AS (PARTITION BY country ORDER BY rating DESC, id), w_running_2 AS (PARTITION BY city, country)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
        FROM with_windows
    ) x
    WHERE row_num = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        rating,
        rating_bucket,
        row_num,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
        NTILE(5) OVER (PARTITION BY country ORDER BY city_stddev_samp_rating, id) AS city_stddev_samp_rating_bucket,
        NTILE(10) OVER (PARTITION BY country ORDER BY next_rating DESC, id) AS next_rating_bucket,
        SUM(city_stddev_samp_rating) OVER (ORDER BY next_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_city_stddev_samp_rating
    FROM latest
)
SELECT
    id,
    country,
    rating,
    rating_bucket,
    row_num,
    city_stddev_samp_rating_bucket,
    running_city_stddev_samp_rating
FROM metrics
ORDER BY row_num DESC;
