WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(store_name) AS store_name_upper
    FROM stores
    WHERE region IS NOT NULL
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.order_status,
        x.amount
    FROM valid s
    JOIN LATERAL (
            SELECT c.status AS order_status, c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CEIL(sqft) AS sqft_whole,
        LOWER(store_name) AS store_name_lower
    FROM with_last
    WHERE opened_date >= DATE '2023-08-09'
      AND sqft < 7526
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_name,
        city,
        opened_date,
        sqft,
        amount,
        sqft_whole,
        store_name_lower
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft, id) AS rank_in_group
        FROM filtered
    ) x
    WHERE rank_in_group = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(sqft_whole) OVER w_ordered AS city_max_sqft_whole,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY city), w_recent AS (PARTITION BY city ORDER BY sqft DESC, id)
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.opened_date,
        s.sqft,
        s.amount,
        s.city_max_sqft_whole,
        s.seq_no,
        top1.store_sale_revenue
    FROM scored s
    LEFT JOIN LATERAL (
            SELECT c.revenue AS store_sale_revenue
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Grouped metrics
SELECT
    DATE_TRUNC('MONTH', opened_date) AS activity_month,
    MIN(amount) AS min_amount,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(seq_no, 0.9) AS seq_no_p90,
    COUNT(*) AS num_rows_2,
    COUNT(*) AS event_count,
    BIT_OR(sqft) AS sqft_bit_or
FROM with_latest
GROUP BY 1;
