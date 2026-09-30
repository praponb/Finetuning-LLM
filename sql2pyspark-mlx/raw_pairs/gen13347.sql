WITH valid AS (
    SELECT
        id,
        country,
        COALESCE(rating, 0) AS rating_filled
    FROM suppliers
    WHERE rating <= 3.1
),
enriched AS (
    SELECT
        id,
        country,
        rating_filled,
        SUM(rating_filled) OVER w_running AS cumulative_rating_filled,
        RANK() OVER w_grp AS rating_filled_rank
    FROM valid
    WINDOW w_running AS (PARTITION BY country ORDER BY rating_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY country ORDER BY rating_filled)
),
latest_match AS (
    SELECT s.*, top1.product_launch_date
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.launch_date >= s.product_launch_date
        )
),
grouped AS (
    SELECT
        country,
        COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
        ROUND(SUM(rating_filled_rank) / NULLIF(COUNT(CASE WHEN country = 'JP' THEN 1 END), 0), 2) AS rating_filled_rank_per_jp
    FROM eligible
    GROUP BY country
)
SELECT country, CASE WHEN country RLIKE '^(SG|DE)$' THEN 'PRIORITY' WHEN country RLIKE '^(TH|US)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS country_zone
FROM grouped
ORDER BY country_zone DESC NULLS LAST, country DESC;
