WITH filtered_claims AS (
    -- Filter claims
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
        DATE_TRUNC('MONTH', claim_date) AS claim_date_month,
        LENGTH(TRIM(claim_type)) AS claim_type_len,
        LOWER(REGEXP_REPLACE(TRIM(claim_type), ' ', '_')) AS claim_type_slug
    FROM claims
    WHERE policy_id >= 3
      AND claimed_amount BETWEEN 3211.4 AND 46461.4
),
grouped AS (
    -- Group-level totals
    SELECT
        YEAR(reported_at) AS reported_at_year,
        claim_type,
        MAX(claim_date_month) AS last_claim_date_month,
        ARRAY_SORT(COLLECT_SET(claim_type)) AS claim_type_list,
        SUM(claim_type_len) AS total_claim_type_len,
        MAX(approved_amount) AS max_approved_amount
    FROM filtered_claims
    WHERE claim_type IN ('theft', 'collision', 'delay')
    GROUP BY YEAR(reported_at), claim_type
),
sequenced AS (
    -- Running and moving aggregates
    SELECT
        reported_at_year,
        claim_type,
        last_claim_date_month,
        claim_type_list,
        total_claim_type_len,
        max_approved_amount,
        NTILE(4) OVER w_reported_at_year_by_max_appr AS max_approved_amount_quartile,
        ROW_NUMBER() OVER (PARTITION BY reported_at_year ORDER BY max_approved_amount, reported_at_year, claim_type) AS seq_no,
        MAX(max_approved_amount) OVER (PARTITION BY reported_at_year ORDER BY max_approved_amount, reported_at_year, claim_type ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS max_approved_amount_max_6_prior,
        LAG(reported_at_year) OVER (PARTITION BY reported_at_year ORDER BY max_approved_amount, reported_at_year, claim_type) AS prev_reported_at_year,
        ROUND(PERCENT_RANK() OVER w_reported_at_year_by_max_appr_3, 3) AS max_approved_amount_percent_rank
    FROM grouped
    WINDOW w_reported_at_year_by_max_appr AS (PARTITION BY reported_at_year ORDER BY max_approved_amount, reported_at_year, claim_type), w_reported_at_year_by_max_appr_3 AS (PARTITION BY reported_at_year ORDER BY max_approved_amount)
),
without_matches AS (
    -- Compare against the overall avg
    SELECT *
    FROM sequenced
    WHERE max_approved_amount_quartile > (SELECT AVG(max_approved_amount_quartile) FROM sequenced)
),
windowed AS (
    -- Previous / next values per key
    SELECT
        reported_at_year,
        claim_type,
        last_claim_date_month,
        claim_type_list,
        max_approved_amount,
        max_approved_amount_quartile,
        max_approved_amount_max_6_prior,
        max_approved_amount_percent_rank,
        ROUND(CUME_DIST() OVER w_seq_no_reported_at_year_by_m, 3) AS max_approved_amount_percent_rank_cume_dist,
        MAX(max_approved_amount_quartile) OVER (PARTITION BY seq_no, reported_at_year ORDER BY max_approved_amount_percent_rank DESC, reported_at_year, claim_type ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_max_approved_amount_quartile_so_far,
        MAX(prev_reported_at_year) OVER (PARTITION BY claim_type ORDER BY max_approved_amount_quartile DESC, reported_at_year, claim_type ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS max_prev_reported_at_year_so_far
    FROM without_matches
    WINDOW w_seq_no_reported_at_year_by_m AS (PARTITION BY seq_no, reported_at_year ORDER BY max_approved_amount_percent_rank DESC)
),
screened AS (
    -- Compare against the overall min
    SELECT *
    FROM windowed
    WHERE max_max_approved_amount_quartile_so_far > (SELECT MIN(max_max_approved_amount_quartile_so_far) FROM windowed)
)
-- Final projection
SELECT
    reported_at_year,
    claim_type,
    last_claim_date_month,
    claim_type_list,
    max_approved_amount_max_6_prior,
    max_max_approved_amount_quartile_so_far,
    max_prev_reported_at_year_so_far
FROM screened
