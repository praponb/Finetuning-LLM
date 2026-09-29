WITH cleaned_policys AS (
    -- Scope the policies rows
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        CONCAT_WS('|', status, product) AS status_product_label,
        YEAR(end_date) AS end_date_year,
        ADD_MONTHS(end_date, 1) AS end_date_plus_1m
    FROM policies
    WHERE holder_id <= 9
      AND start_date < DATE '2025-02-13'
),
deduped AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status_product_label,
        end_date_year,
        end_date_plus_1m
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY end_date DESC, id DESC) AS rn
        FROM cleaned_policys
    ) ranked
    WHERE rn = 1
),
product_stats AS (
    -- Avg end_date_year per product
    SELECT product, AVG(end_date_year) AS avg_end_date_year
    FROM deduped
    GROUP BY product
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_end_date_year,
        ROUND(s.end_date_year - g.avg_end_date_year, 2) AS end_date_year_minus_avg
    FROM deduped s
    JOIN product_stats g ON g.product = s.product
    WHERE s.end_date_year > g.avg_end_date_year
),
latest AS (
    -- Keep the latest row per holder_id
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        status_product_label,
        end_date_year,
        end_date_plus_1m,
        avg_end_date_year,
        end_date_year_minus_avg
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY holder_id ORDER BY end_date DESC, id DESC) AS rn
        FROM compared
    ) ranked
    WHERE rn = 1
)
SELECT
    product,
    end_date_year,
    COUNT(*) FILTER (WHERE product IN ('TRAVEL', 'HEALTH', 'LIFE')) AS n_product_in_travel_health_lif,
    MIN(avg_end_date_year) AS min_avg_end_date_year
FROM latest
GROUP BY ROLLUP(product, end_date_year)
ORDER BY product NULLS LAST, end_date_year NULLS LAST
