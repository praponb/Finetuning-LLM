WITH valid_claims AS (
    -- Scope the claims rows
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_date,
        claim_type,
        claimed_amount,
        approved_amount,
        status,
        reported_at,
        QUARTER(reported_at) AS reported_at_quarter,
        LOWER(TRIM(claim_type)) AS claim_type_clean
    FROM claims
    WHERE adjuster_id <= 5
),
totals AS (
    -- Group-level totals
    SELECT
        YEAR(claim_date) AS claim_date_year,
        SUM(approved_amount) AS total_approved_amount,
        COUNT(*) AS row_count,
        ARRAY_SORT(COLLECT_SET(status)) AS status_list
    FROM valid_claims
    GROUP BY YEAR(claim_date)
),
with_activity AS (
    -- Compare against the overall min
    SELECT *
    FROM totals
    WHERE total_approved_amount > (SELECT MIN(total_approved_amount) FROM totals)
),
with_windows AS (
    -- Window metrics over several partitions
    SELECT
        *,
        MIN(total_approved_amount) OVER (PARTITION BY row_count) AS row_count_min_total_approved_amount,
        MIN(claim_date_year) OVER (PARTITION BY row_count) AS row_count_min_claim_date_year,
        DENSE_RANK() OVER (PARTITION BY row_count ORDER BY row_count) AS row_count_dense_rank,
        COUNT(*) OVER (PARTITION BY row_count) AS rows_in_row_count,
        FIRST_VALUE(claim_date_year) IGNORE NULLS OVER (PARTITION BY row_count ORDER BY row_count, claim_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_claim_date_year
    FROM with_activity
    WINDOW w_row_count AS (PARTITION BY row_count)
),
first_rows AS (
    -- Top 5 per group
    SELECT
        claim_date_year,
        total_approved_amount,
        row_count,
        status_list,
        row_count_min_total_approved_amount,
        row_count_min_claim_date_year,
        rows_in_row_count,
        first_known_claim_date_year
    FROM with_windows
    WHERE row_count_dense_rank <= 5
),
windowed AS (
    -- Window metrics over several partitions
    SELECT
        *,
        LEAD(total_approved_amount) OVER (PARTITION BY first_known_claim_date_year, claim_date_year ORDER BY first_known_claim_date_year ASC, claim_date_year) AS next_total_approved_amount,
        NTILE(3) OVER (ORDER BY total_approved_amount, claim_date_year) AS total_approved_amount_tercile,
        LAST_VALUE(total_approved_amount) OVER (PARTITION BY first_known_claim_date_year, claim_date_year ORDER BY first_known_claim_date_year ASC, claim_date_year ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_total_approved_amount,
        ROUND(row_count / NULLIF(SUM(row_count) OVER (PARTITION BY claim_date_year), 0), 4) AS row_count_share_of_claim_date_year,
        ROUND(CUME_DIST() OVER (ORDER BY rows_in_row_count ASC), 3) AS rows_in_row_count_cume_dist,
        ROUND(total_approved_amount / NULLIF(SUM(total_approved_amount) OVER (PARTITION BY claim_date_year), 0), 4) AS total_approved_amount_share_of_claim_date_year
    FROM first_rows
)
SELECT claim_date_year, field, reading
FROM (
    SELECT claim_date_year, rows_in_row_count_cume_dist, total_approved_amount
    FROM windowed
) src
UNPIVOT (
    reading FOR field IN (rows_in_row_count_cume_dist, total_approved_amount)
) unp
ORDER BY claim_date_year, field
