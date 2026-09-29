WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(sqft, 0) AS sqft_rounded
    FROM stores
    WHERE region NOT IN ('West', 'North', 'South')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        sqft_rounded,
        LOWER(store_name) AS store_name_lower,
        COALESCE(region, 'NONE') AS resolved_region
    FROM base
    WHERE opened_date > DATE '2023-09-30'
      AND city IN ('Berlin', 'New York', 'Singapore')
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.inventory_last_restock,
        latest.inventory_quantity
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.last_restock AS inventory_last_restock, c.quantity AS inventory_quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        region,
        sqft_rounded,
        store_name_lower,
        resolved_region,
        inventory_last_restock,
        inventory_quantity,
        AVG(sqft_rounded) OVER (ORDER BY sqft_rounded, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS sqft_rounded_avg_5_prior,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY inventory_last_restock DESC, id) AS event_seq,
        AVG(sqft_rounded) OVER (PARTITION BY MONTH(inventory_last_restock), region ORDER BY inventory_quantity, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_rounded_avg_6_prior,
        SUM(sqft_rounded) OVER (PARTITION BY city ORDER BY inventory_last_restock, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft_rounded,
        SUM(inventory_quantity) OVER (PARTITION BY city ORDER BY inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_inventory_quantity
    FROM latest_match
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        region,
        store_name_lower,
        inventory_quantity,
        event_seq,
        cumulative_sqft_rounded,
        running_inventory_quantity,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY event_seq DESC, id) AS event_seq_2,
        SUM(inventory_quantity) OVER (PARTITION BY YEAR(inventory_last_restock), city) AS inventory_last_restock_year_sum_inventory_quantity,
        AVG(cumulative_sqft_rounded) OVER (ORDER BY running_inventory_quantity, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cumulative_sqft_rounded_avg_2
    FROM with_windows
)
-- Grouped metrics
SELECT
    CASE WHEN running_inventory_quantity >= 1166 THEN 'PREMIUM' ELSE 'STANDARD' END AS running_inventory_quantity_tier,
    SUM(inventory_quantity) FILTER (WHERE region = 'West') AS west_total_inventory_quantity,
    ROUND(SUM(cumulative_sqft_rounded_avg_2) / NULLIF(COUNT(CASE WHEN city = 'Tokyo' THEN 1 END), 0), 2) AS cumulative_sqft_rounded_avg_2_per_tokyo
FROM scored
GROUP BY 1
ORDER BY cumulative_sqft_rounded_avg_2_per_tokyo DESC;
