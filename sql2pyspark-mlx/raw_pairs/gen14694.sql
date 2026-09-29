WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        INITCAP(TRIM(region)) AS region_title,
        COALESCE(sqft, 0) AS sqft_filled
    FROM stores
    WHERE store_name LIKE 'Sia%'
),
cleaned_stores AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        region_title,
        sqft_filled,
        REGEXP_REPLACE(store_name, ' ', '-') AS store_name_slug
    FROM cleaned
    WHERE opened_date >= TO_DATE('2023-07-06')
      AND store_name LIKE '%Express%'
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_name,
        s.opened_date,
        s.sqft_filled,
        top1.quantity
    FROM cleaned_stores s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        sqft_filled,
        ROW_NUMBER() OVER (ORDER BY sqft_filled ASC, id ASC) AS seq_no,
        RANK() OVER (PARTITION BY DATE_TRUNC('month', opened_date) ORDER BY sqft_filled DESC) AS sqft_filled_rank,
        LEAD(quantity, 1) OVER (ORDER BY quantity ASC, id ASC) AS next_quantity,
        LAG(sqft_filled, 1) OVER (ORDER BY sqft_filled ASC, id ASC) AS prev_sqft_filled,
        AVG(quantity) OVER (ORDER BY quantity ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS quantity_avg_6_prior
    FROM with_last
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(prev_sqft_filled, TRUE) OVER (ORDER BY sqft_filled ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_prev_sqft_filled,
        LEAD(sqft_filled, 1) OVER (ORDER BY quantity_avg_6_prior ASC, id ASC) AS next_sqft_filled,
        CUME_DIST() OVER (ORDER BY prev_sqft_filled ASC NULLS FIRST) AS prev_sqft_filled_cume_dist
    FROM scored
    WHERE sqft_filled <= 25
),
with_last_stores AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.discount,
        x.order_channel
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.discount, c.channel AS order_channel
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
-- Grouped metrics
SELECT
    CASE WHEN quantity_avg_6_prior >= 203 THEN 'PREMIUM' ELSE 'STANDARD' END AS quantity_avg_6_prior_tier,
    CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END AS seq_no_tier,
    COUNT(*) FILTER (WHERE order_channel = 'partner') AS partner_rows,
    ROUND(SUM(quantity_avg_6_prior) / NULLIF(COUNT(CASE WHEN order_channel = 'web' THEN 1 END), 0), 2) AS quantity_avg_6_prior_per_web,
    ROUND(AVG(last_known_prev_sqft_filled), 2) AS avg_last_known_prev_sqft_filled,
    COUNT(*) AS num_rows,
    ROUND(AVG(sqft_filled), 2) AS avg_sqft_filled
FROM with_last_stores
GROUP BY ROLLUP (CASE WHEN quantity_avg_6_prior >= 203 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN seq_no >= 8 THEN 'HIGH' ELSE 'LOW' END)
HAVING ROUND(AVG(sqft_filled), 2) < 1000000;
