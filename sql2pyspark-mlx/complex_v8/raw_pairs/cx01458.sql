WITH prepared_policys AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        COALESCE(status, 'UNKNOWN') AS status_or_unknown,
        DAYOFWEEK(end_date) AS end_date_dayofweek,
        COALESCE(premium, 0.0) AS premium_filled
    FROM policies
    WHERE YEAR(start_date) = 2025
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        *,
        ROUND(end_date_dayofweek - AVG(end_date_dayofweek) OVER (PARTITION BY product), 2) AS end_date_dayofweek_vs_product_avg,
        NTH_VALUE(premium_filled, 2) OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS second_premium_filled,
        FIRST_VALUE(end_date) IGNORE NULLS OVER (ORDER BY end_date DESC, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_end_date,
        ROUND(AVG(premium_filled) OVER (PARTITION BY product), 2) AS product_avg_premium_filled
    FROM prepared_policys
),
unique_rows AS (
    -- Keep the earliest row per status_or_unknown, holder_id
    SELECT
        id,
        holder_id,
        product,
        premium,
        start_date,
        end_date,
        status_or_unknown,
        end_date_dayofweek,
        premium_filled,
        end_date_dayofweek_vs_product_avg,
        second_premium_filled,
        first_known_end_date,
        product_avg_premium_filled
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY status_or_unknown, holder_id ORDER BY end_date, id) AS rn
        FROM sequenced
    ) ranked
    WHERE rn = 1
)
SELECT id, field, reading
FROM (
    SELECT id, product_avg_premium_filled, end_date_dayofweek_vs_product_avg, premium_filled, premium
    FROM unique_rows
) src
UNPIVOT (
    reading FOR field IN (product_avg_premium_filled, end_date_dayofweek_vs_product_avg, premium_filled, premium)
) unp
