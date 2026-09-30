WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(country) AS country_upper,
        UPPER(city) AS city_upper
    FROM suppliers
    WHERE rating <= 2.6
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM filtered
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY rating DESC) = 1
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS row_seq,
        ROW_NUMBER() OVER w_grp AS row_seq_2,
        LAG(rating, 1) OVER (PARTITION BY country, city ORDER BY rating ASC, id ASC) AS prev_rating
    FROM deduped
    WINDOW w_grp AS (PARTITION BY country ORDER BY rating ASC, id ASC)
),
enriched_suppliers AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share,
        ROUND(row_seq_2 / SUM(row_seq_2) OVER (PARTITION BY country), 4) AS row_seq_2_share,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share_2
    FROM enriched
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(row_seq_2) OVER (PARTITION BY city) AS city_stddev_samp_row_seq_2,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id ASC) AS row_seq_3,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY prev_rating DESC, id ASC) AS event_seq,
        ROW_NUMBER() OVER (ORDER BY row_seq_2_share ASC, id ASC) AS row_seq_4
    FROM enriched_suppliers
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.product_brand,
        x.category
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.brand AS product_brand, c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY city, product_brand ORDER BY row_seq_2 DESC, id ASC) AS row_seq_2_bucket,
        PERCENT_RANK() OVER (PARTITION BY category, country ORDER BY row_seq_3 DESC) AS row_seq_3_percent_rank,
        AVG(event_seq) OVER (ORDER BY event_seq ASC, id ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS event_seq_avg_2
    FROM with_last
)
SELECT
    id,
    country,
    city,
    country_upper,
    city_upper,
    row_seq_2,
    prev_rating,
    rating_share,
    row_seq_2_share
FROM metrics;
