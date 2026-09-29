WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LENGTH(city) AS city_length,
        UPPER(country) AS country_upper
    FROM suppliers
    WHERE country IN ('US', 'TH', 'DE')
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.rating,
        s.country_upper,
        x.price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        country_upper,
        price,
        LAG(price) OVER w_running AS prev_price,
        LAG(rating) OVER w_running AS prev_rating,
        ROW_NUMBER() OVER w_running AS seq_no
    FROM with_last
    WINDOW w_running AS (PARTITION BY city ORDER BY rating ASC, id ASC)
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.launch_date BETWEEN '2025-06-27' AND '2025-07-01'
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(prev_rating) OVER (ORDER BY seq_no ASC, id ASC) AS next_prev_rating,
        LAG(prev_rating) OVER (ORDER BY seq_no ASC, id ASC) AS prev_prev_rating,
        LAG(price) OVER (ORDER BY seq_no ASC, id ASC) AS prev_price_2,
        LEAD(price) OVER (ORDER BY seq_no ASC, id ASC) AS next_price,
        ROW_NUMBER() OVER (ORDER BY seq_no ASC, id ASC) AS event_seq
    FROM not_flagged
    WHERE prev_rating > 3.6
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(prev_price) AS prev_price_whole
    FROM with_windows
    WHERE prev_price BETWEEN 770 AND 1110
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(5) OVER (ORDER BY seq_no DESC, id ASC) AS seq_no_bucket,
        CUME_DIST() OVER (ORDER BY prev_rating DESC NULLS LAST) AS prev_rating_cume_dist,
        NTILE(5) OVER (ORDER BY prev_price_whole ASC, id ASC) AS prev_price_whole_bucket
    FROM cleaned
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN prev_price >= 1200 THEN 'HIGH' ELSE 'LOW' END AS prev_price_tier,
        CASE WHEN next_prev_rating >= 4.4 THEN 'LARGE' ELSE 'SMALL' END AS next_prev_rating_tier,
        ROUND(AVG(prev_price), 2) AS avg_prev_price,
        AVG(prev_price_whole_bucket) AS avg_prev_price_whole_bucket,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
        MAX(seq_no) AS max_seq_no,
        APPROX_PERCENTILE(prev_price_2, ARRAY(0.25, 0.5, 0.75)) AS prev_price_2_quartiles,
        GROUPING(CASE WHEN prev_price >= 1200 THEN 'HIGH' ELSE 'LOW' END) AS grouping_level
    FROM scored
    GROUP BY GROUPING SETS ((CASE WHEN prev_price >= 1200 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN next_prev_rating >= 4.4 THEN 'LARGE' ELSE 'SMALL' END), (CASE WHEN prev_price >= 1200 THEN 'HIGH' ELSE 'LOW' END), ())
)
SELECT
    prev_price_tier,
    next_prev_rating_tier,
    avg_prev_price,
    avg_prev_price_whole_bucket,
    num_rows,
    event_seq_quartiles,
    max_seq_no,
    prev_price_2_quartiles
FROM grouped;
