WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS element
),
cleaned AS (
    SELECT *, TRIM(currency) AS clean_currency
    FROM lines
    WHERE effective_from >= '2023-04-01'
),
valid AS (
    SELECT
        effective_from,
        currency,
        clean_currency,
        COALESCE(element, 0) AS element_filled
    FROM cleaned
    WHERE element IS NOT NULL
),
enriched AS (
    SELECT
        effective_from,
        clean_currency,
        MAX(element_filled) OVER w_running AS effective_from_year_max_element_filled,
        COUNT(element_filled) OVER (PARTITION BY currency) AS currency_count_element_filled,
        RANK() OVER (PARTITION BY currency ORDER BY element_filled ASC NULLS FIRST) AS element_filled_rank,
        DENSE_RANK() OVER w_part AS element_filled_dense_rank,
        PERCENT_RANK() OVER w_hist AS element_filled_percent_rank
    FROM valid
    WHERE MONTH(effective_from) IN (5, 7, 12)
    WINDOW w_running AS (PARTITION BY YEAR(effective_from)), w_part AS (PARTITION BY YEAR(effective_from), currency ORDER BY element_filled DESC NULLS FIRST), w_hist AS (PARTITION BY currency ORDER BY element_filled DESC NULLS FIRST)
)
SELECT
    CASE WHEN element_filled_dense_rank >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS element_filled_dense_rank_tier,
    COUNT(*) AS num_rows,
    ROUND(AVG(element_filled_percent_rank), 2) AS avg_element_filled_percent_rank,
    ROUND(AVG(effective_from_year_max_element_filled), 2) AS avg_effective_from_year_max_element_filled
FROM enriched
GROUP BY CASE WHEN element_filled_dense_rank >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING num_rows > 1
ORDER BY element_filled_dense_rank_tier DESC
