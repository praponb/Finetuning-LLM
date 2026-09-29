WITH filtered AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claimed_amount,
        approved_amount,
        reported_at,
        TO_DATE(reported_at) AS reported_at_date,
        QUARTER(reported_at) AS reported_at_quarter
    FROM claims
),
totals AS (
    -- Summarise per group
    SELECT
        DATE_FORMAT(reported_at, 'yyyy-MM') AS reported_at_ym,
        reported_at_quarter,
        ROUND(SUM(approved_amount) / NULLIF(SUM(claimed_amount), 0), 3) AS approved_amount_to_claimed_amount_ratio,
        COUNT(DISTINCT id) AS distinct_ids
    FROM filtered
    WHERE reported_at_quarter < 3
    GROUP BY DATE_FORMAT(reported_at, 'yyyy-MM'), reported_at_quarter
    HAVING SUM(claimed_amount) > 10518.4
),
without_matches AS (
    -- Compare against the overall min
    SELECT *
    FROM totals
    WHERE approved_amount_to_claimed_amount_ratio > (SELECT MIN(approved_amount_to_claimed_amount_ratio) FROM totals)
),
scored AS (
    -- Label and score rows
    SELECT *, ABS(distinct_ids - 6) AS distinct_ids_distance
    FROM without_matches
    WHERE approved_amount_to_claimed_amount_ratio < 9.0
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM scored
    WHERE distinct_ids_distance > (SELECT MIN(distinct_ids_distance) FROM scored)
),
derived AS (
    -- Derive helper columns
    SELECT
        reported_at_ym,
        reported_at_quarter,
        distinct_ids_distance,
        FLOOR(distinct_ids_distance / 2) * 2 AS distinct_ids_distance_bucket,
        FLOOR(distinct_ids / 2) * 2 AS distinct_ids_bucket,
        LENGTH(TRIM(reported_at_ym)) AS reported_at_ym_len
    FROM screened
)
-- Final projection
SELECT
    reported_at_ym,
    reported_at_quarter,
    distinct_ids_distance,
    distinct_ids_bucket
FROM derived
WHERE distinct_ids_distance < 8
ORDER BY distinct_ids_bucket DESC NULLS FIRST, reported_at_ym
