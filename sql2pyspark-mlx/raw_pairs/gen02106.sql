WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CASE WHEN country = 'US' THEN 1 ELSE 0 END AS is_us,
        COALESCE(city, 'NONE') AS city_filled
    FROM suppliers
    WHERE supplier_name LIKE '%Pacif%'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(rating, 0) AS rating_filled
    FROM filtered
    WHERE supplier_name LIKE '%Sia%'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        city,
        is_us,
        rating_filled
    FROM cleaned
    QUALIFY RANK() OVER (PARTITION BY city ORDER BY rating_filled DESC NULLS LAST) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        is_us * rating_filled AS is_us_times_rating_filled,
        COALESCE(city, 'OTHER') AS resolved_city
    FROM ranked
    WHERE city IS NOT NULL
      AND is_us < 74
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.is_us,
        s.rating_filled,
        latest.price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        is_us,
        rating_filled,
        LEAST(rating_filled, is_us) AS least_rating_filled_is_us
    FROM with_last
    WHERE city = 'Chiang Mai'
)
SELECT id, least_rating_filled_is_us
FROM valid
ORDER BY id DESC NULLS FIRST
