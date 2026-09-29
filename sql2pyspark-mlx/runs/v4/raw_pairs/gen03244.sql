WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(budget, 0) AS budget_filled
    FROM departments
    WHERE dept_name IS NOT NULL
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_filled DESC, id) AS rank_in_group
        FROM cleaned
    ) ranked
    WHERE rank_in_group = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        budget,
        ROUND(budget_filled * 1.07, 2) AS adjusted_budget_filled
    FROM latest
    WHERE rank_in_group > 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM base
    EXCEPT ALL
    SELECT id
    FROM departments
    WHERE location NOT IN ('Chiang Mai', 'Bangkok', 'New York')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    INNER JOIN matched_keys k ON s.id = k.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        budget,
        adjusted_budget_filled,
        RANK() OVER (ORDER BY adjusted_budget_filled NULLS FIRST) AS adjusted_budget_filled_rank,
        CUME_DIST() OVER (ORDER BY adjusted_budget_filled DESC NULLS FIRST) AS adjusted_budget_filled_cume_dist
    FROM reconciled
)
SELECT
    budget,
    adjusted_budget_filled_rank,
    adjusted_budget_filled_cume_dist
FROM enriched
