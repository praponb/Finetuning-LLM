WITH prepared_policys AS (
    -- Filter policies
    SELECT
        id,
        holder_id,
        product,
        start_date,
        end_date,
        CONCAT(product, '#', CAST(holder_id AS STRING)) AS product_ref
    FROM policies
    WHERE (end_date >= DATE '2026-02-26' OR product = 'TRAVEL')
),
summary AS (
    -- Summarise per group
    SELECT
        YEAR(end_date) AS end_date_year,
        SUM(CASE WHEN product = 'LIFE' THEN 1 ELSE 0 END) AS n_product_life,
        COUNT(DISTINCT id) AS distinct_ids
    FROM prepared_policys
    GROUP BY 1
),
tagged AS (
    -- Derive helper columns
    SELECT
        end_date_year,
        n_product_life,
        distinct_ids,
        n_product_life - distinct_ids AS n_product_life_minus_distinct_ids,
        GREATEST(end_date_year, distinct_ids) AS greatest_end_date_year_distinct_ids
    FROM summary
    WHERE end_date_year <= 2025
),
sequenced AS (
    -- Window metrics over several partitions
    SELECT
        end_date_year,
        n_product_life,
        greatest_end_date_year_distinct_ids,
        ROUND(PERCENT_RANK() OVER w_end_date_year_n_product_life, 3) AS distinct_ids_percent_rank,
        end_date_year - LAG(end_date_year) OVER (PARTITION BY distinct_ids, n_product_life ORDER BY greatest_end_date_year_distinct_ids DESC, end_date_year) AS end_date_year_change
    FROM tagged
    WINDOW w_end_date_year_n_product_life AS (PARTITION BY end_date_year, n_product_life ORDER BY distinct_ids DESC)
),
sequenced_2 AS (
    -- Rank and compare rows within groups
    SELECT
        end_date_year,
        n_product_life,
        greatest_end_date_year_distinct_ids,
        distinct_ids_percent_rank,
        SUM(distinct_ids_percent_rank) OVER (ORDER BY distinct_ids_percent_rank DESC, end_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_distinct_ids_percent_rank,
        SUM(end_date_year_change) OVER (ORDER BY n_product_life DESC, end_date_year ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS end_date_year_change_sum_2,
        MAX(distinct_ids_percent_rank) OVER w_end_date_year AS end_date_year_max_distinct_ids_percent_rank,
        FIRST_VALUE(end_date_year_change) OVER (ORDER BY n_product_life DESC, end_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_end_date_year_change,
        MAX(end_date_year) OVER w_end_date_year AS end_date_year_max_end_date_year,
        ROUND(distinct_ids_percent_rank - AVG(distinct_ids_percent_rank) OVER (PARTITION BY end_date_year), 2) AS distinct_ids_percent_rank_vs_end_date_year_avg
    FROM sequenced
    WINDOW w_end_date_year AS (PARTITION BY end_date_year)
),
filtered AS (
    -- Compare against the overall min
    SELECT *
    FROM sequenced_2
    WHERE distinct_ids_percent_rank > (SELECT MIN(distinct_ids_percent_rank) FROM sequenced_2)
),
with_activity AS (
    -- Compare against the overall max
    SELECT *
    FROM filtered
    WHERE end_date_year_max_distinct_ids_percent_rank < (SELECT MAX(end_date_year_max_distinct_ids_percent_rank) FROM filtered)
)
-- Final projection
SELECT
    end_date_year,
    n_product_life,
    greatest_end_date_year_distinct_ids,
    distinct_ids_percent_rank,
    running_distinct_ids_percent_rank,
    end_date_year_change_sum_2,
    end_date_year_max_distinct_ids_percent_rank,
    end_date_year_max_end_date_year,
    distinct_ids_percent_rank_vs_end_date_year_avg,
    ROUND(n_product_life / NULLIF(end_date_year, 0), 3) AS n_product_life_per_end_date_year,
    n_product_life + end_date_year AS n_product_life_plus_end_date_year
FROM with_activity
