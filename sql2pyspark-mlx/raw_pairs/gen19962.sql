WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(effective_from, 3) AS effective_from_plus_months
    FROM flattened
    WHERE effective_from > '2025-04-22'
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        RANK() OVER w_ordered AS val_rank
    FROM valid
    WHERE val BETWEEN 28 AND 59
    WINDOW w_ordered AS (ORDER BY val ASC NULLS LAST)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        PERCENT_RANK() OVER (ORDER BY val_rank ASC NULLS FIRST) AS val_rank_percent_rank,
        AVG(val_rank) OVER (PARTITION BY YEAR(effective_from)) AS effective_from_year_avg_val_rank,
        SUM(val_rank) OVER (PARTITION BY YEAR(effective_from)) AS effective_from_year_sum_val_rank,
        PERCENT_RANK() OVER (ORDER BY val_rank DESC NULLS FIRST) AS val_rank_percent_rank_2,
        RANK() OVER (ORDER BY val_rank ASC) AS val_rank_rank
    FROM metrics
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        effective_from,
        effective_from_year_avg_val_rank,
        val_rank_percent_rank_2,
        val_rank_rank,
        DATE_TRUNC('year', effective_from) AS effective_from_period
    FROM scored
    WHERE effective_from > TO_DATE('2024-04-22')
)
SELECT
    id,
    effective_from_year_avg_val_rank,
    val_rank_percent_rank_2
FROM base
ORDER BY effective_from_year_avg_val_rank DESC, id ASC
