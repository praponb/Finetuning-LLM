WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_FORMAT(opened_date, 'yyyy-MM-dd') AS opened_date_label
    FROM stores
    WHERE opened_date BETWEEN '2025-05-01' AND '2025-08-26'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY city ORDER BY sqft, id) AS sqft_bucket,
        CUME_DIST() OVER (PARTITION BY DATE_TRUNC('WEEK', opened_date), region ORDER BY sqft DESC NULLS FIRST) AS sqft_cume_dist,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        LEAD(sqft, 1) OVER (PARTITION BY city ORDER BY opened_date, id) AS next_sqft
    FROM valid
),
valid_stores AS (
    -- Keep recent, valid rows
    SELECT
        *,
        REPLACE(store_name, ' ', '-') AS store_name_slug,
        next_sqft + sqft_share AS next_sqft_plus_sqft_share
    FROM scored
    WHERE sqft_bucket < 8
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.opened_date,
        s.sqft,
        s.sqft_bucket,
        s.sqft_cume_dist,
        s.sqft_share,
        s.next_sqft,
        s.next_sqft_plus_sqft_share,
        x.channel,
        x.order_amount
    FROM valid_stores s
    LEFT JOIN LATERAL (
            SELECT c.channel, c.amount AS order_amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM with_last a
    INNER JOIN promotions pr
        ON COALESCE(a.channel, 'web') = pr.channel
       AND a.opened_date >= pr.starts_on
       AND a.opened_date < pr.ends_on + INTERVAL 7 DAYS
)
-- Multi-level aggregation
SELECT
    promo_code,
    CASE WHEN order_amount >= 1700 THEN 'LARGE' ELSE 'SMALL' END AS order_amount_tier,
    BIT_OR(sqft) AS sqft_bit_or,
    BIT_OR(sqft) AS sqft_bit_or_2,
    COUNT(*) AS cnt,
    ROUND(SUM(order_amount) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS order_amount_per_store,
    MAX(sqft_share) AS max_sqft_share,
    ROUND(AVG(sqft_cume_dist), 2) AS avg_sqft_cume_dist
FROM matched
GROUP BY promo_code, order_amount_tier
ORDER BY order_amount_per_store DESC;
