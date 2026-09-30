WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(is_flagged AS INT) AS is_flagged_flag,
        CAST(is_flagged AS INT) AS is_flagged_flag_2,
        COALESCE(merchant, 'NONE') AS resolved_merchant
    FROM transactions
    WHERE txn_time >= TO_DATE('2023-05-31')
      AND merchant IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY is_flagged_flag DESC NULLS LAST, id) AS rn
    FROM base
    QUALIFY rn = 1
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        is_flagged,
        is_flagged_flag_2,
        rn,
        LEAD(is_flagged_flag_2, 1, 0) OVER (PARTITION BY merchant ORDER BY txn_time, id) AS next_is_flagged_flag_2,
        LAST_VALUE(is_flagged_flag_2) IGNORE NULLS OVER (PARTITION BY merchant ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_is_flagged_flag_2
    FROM ranked
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        is_flagged,
        is_flagged_flag_2,
        rn,
        next_is_flagged_flag_2,
        last_known_is_flagged_flag_2,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM enriched
    WHERE txn_type = 'DEBIT'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(is_flagged_flag_2) OVER (ORDER BY last_known_is_flagged_flag_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_is_flagged_flag_2,
        MIN(is_flagged_flag_2) OVER (ORDER BY last_known_is_flagged_flag_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_is_flagged_flag_2,
        SUM(is_flagged_flag_2) OVER (ORDER BY last_known_is_flagged_flag_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_is_flagged_flag_2,
        COUNT(is_flagged_flag_2) OVER (ORDER BY last_known_is_flagged_flag_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_is_flagged_flag_2
    FROM cleaned
    WHERE is_flagged = TRUE
)
SELECT
    id,
    is_flagged,
    is_flagged_flag_2,
    rn,
    next_is_flagged_flag_2,
    is_flagged_flag,
    running_avg_is_flagged_flag_2,
    running_sum_is_flagged_flag_2
FROM scored
