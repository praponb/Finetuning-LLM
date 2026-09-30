WITH valid AS (
    SELECT *, CEIL(cost) AS cost_whole
    FROM products
    WHERE launch_date >= DATE '2023-05-13'
      AND brand IS DISTINCT FROM 'Umbrella'
),
joined AS (
    SELECT
        e.*,
        s.country,
        s.city
    FROM valid e
    LEFT JOIN suppliers s
        ON e.supplier_id <=> s.id
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city, supplier_id ORDER BY cost DESC, id) AS rank_in_group
    FROM joined
    QUALIFY rank_in_group = 1
),
latest AS (
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        country
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost, id) = 1
),
with_last AS (
    SELECT
        s.id,
        s.tags,
        x.revenue
    FROM latest s
    JOIN LATERAL (
            SELECT c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
              AND c.sale_date <= s.launch_date
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    tags,
    revenue,
    ROW_NUMBER() OVER w_hist AS row_seq,
    LEAD(revenue, 1) OVER w_hist AS next_revenue,
    LAST_VALUE(revenue) IGNORE NULLS OVER (ORDER BY revenue, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_revenue
FROM with_last
WHERE ARRAY_CONTAINS(tags, 'premium')
WINDOW w_hist AS (ORDER BY revenue, id);
