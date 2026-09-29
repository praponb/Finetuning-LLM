WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        product_name,
        category,
        price,
        cost,
        launch_date,
        tags,
        supplier_id,
        TRIM(category) AS clean_category
    FROM products
    WHERE launch_date > TO_DATE('2023-03-24')
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('week', launch_date) AS launch_date_period,
        TRIM(category) AS clean_category_2
    FROM valid
    WHERE supplier_id IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        category,
        price,
        launch_date,
        tags,
        supplier_id,
        clean_category,
        launch_date_period,
        clean_category_2,
        LAST_VALUE(cost, TRUE) OVER (PARTITION BY supplier_id, category ORDER BY launch_date_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_cost,
        CORR(cost, price) OVER (PARTITION BY category) AS corr_cost_price
    FROM base
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(last_known_cost) OVER (PARTITION BY supplier_id, category ORDER BY last_known_cost ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_last_known_cost,
        SUM(last_known_cost) OVER (PARTITION BY YEAR(launch_date), supplier_id ORDER BY launch_date_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_last_known_cost,
        FIRST_VALUE(price, TRUE) OVER (PARTITION BY category ORDER BY launch_date_period ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price
    FROM metrics
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.product_id = s.id
              AND ch.last_restock <= s.launch_date
        )
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM eligible
    EXCEPT
    SELECT id
    FROM products
    WHERE stock_qty BETWEEN 194 AND 327
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM eligible s
    INNER JOIN matched_keys k ON s.id = k.id
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT supplier_id, id
    FROM reconciled
    INTERSECT ALL
    SELECT supplier_id, id
    FROM products
    WHERE brand NOT IN ('Stark', 'Globex', 'Umbrella')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM reconciled s
    INNER JOIN remaining_keys k ON s.supplier_id = k.supplier_id AND s.id = k.id
)
SELECT
    id,
    price,
    launch_date,
    tags,
    clean_category,
    launch_date_period,
    clean_category_2,
    cumulative_last_known_cost
FROM verified
ORDER BY launch_date_period ASC NULLS FIRST
