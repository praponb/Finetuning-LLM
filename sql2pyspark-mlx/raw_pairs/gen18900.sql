WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS rank_in_group
    FROM store_sales
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Attach products attributes
    SELECT e.*, p.cost
    FROM latest e
    LEFT JOIN products p
        ON e.product_id = p.id
),
enriched_store_sales AS (
    -- Attach products attributes
    SELECT
        a.id,
        a.store_id,
        a.product_id,
        a.sale_date,
        a.rank_in_group,
        a.cost,
        d.product_name
    FROM enriched a
    LEFT JOIN products d
        ON a.product_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        rank_in_group,
        cost,
        product_name,
        GREATEST(rank_in_group, cost) AS greatest_rank_in_group_cost
    FROM enriched_store_sales
    WHERE sale_date >= DATE '2023-08-31'
),
latest_store_sales AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        sale_date,
        rank_in_group,
        cost,
        greatest_rank_in_group_cost
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY cost DESC NULLS LAST, id) = 1
),
joined AS (
    -- Attach stores attributes
    SELECT
        src.id,
        src.store_id,
        src.sale_date,
        src.rank_in_group,
        src.cost,
        src.greatest_rank_in_group_cost,
        s.city,
        s.sqft
    FROM latest_store_sales src
    LEFT JOIN stores s
        ON src.store_id = s.id
)
SELECT
    id,
    store_id,
    sale_date,
    cost,
    city,
    sqft
FROM joined
ORDER BY sale_date DESC;
