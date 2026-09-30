WITH cleaned_claims AS (
    -- Keep valid rows and derive helper columns
    SELECT
        id,
        policy_id,
        adjuster_id,
        claim_type,
        approved_amount,
        status,
        reported_at,
        DATE_TRUNC('HOUR', reported_at) AS reported_at_hour,
        CASE WHEN claimed_amount >= 33690.0 THEN 'HIGH' WHEN claimed_amount >= 18924.4 THEN 'MEDIUM' ELSE 'LOW' END AS claimed_amount_tier,
        approved_amount + claimed_amount AS approved_amount_plus_claimed_amount
    FROM claims
    WHERE MONTH(reported_at) IN (2, 3)
),
claimed_amount_tier_stats AS (
    -- Avg approved_amount_plus_claimed_amount per claimed_amount_tier
    SELECT claimed_amount_tier, AVG(approved_amount_plus_claimed_amount) AS avg_approved_amount_plus_claimed_amount
    FROM cleaned_claims
    GROUP BY claimed_amount_tier
),
compared AS (
    -- Rows compared with their group statistic
    SELECT
        s.*,
        g.avg_approved_amount_plus_claimed_amount,
        ROUND(s.approved_amount_plus_claimed_amount - g.avg_approved_amount_plus_claimed_amount, 2) AS approved_amount_plus_claimed_amount_minus_avg
    FROM cleaned_claims s
    JOIN claimed_amount_tier_stats g ON g.claimed_amount_tier = s.claimed_amount_tier
    WHERE s.approved_amount_plus_claimed_amount > g.avg_approved_amount_plus_claimed_amount
),
per_group AS (
    -- Summarise per group
    SELECT
        claim_type,
        status,
        COUNT(*) AS cnt,
        MIN(reported_at_hour) AS first_reported_at_hour,
        ROUND(STDDEV(avg_approved_amount_plus_claimed_amount), 2) AS stddev_avg_approved_amount_plus_claimed_amount
    FROM compared
    GROUP BY claim_type, status
    HAVING COUNT(*) >= 2
),
metrics AS (
    -- Rank and compare rows within groups
    SELECT
        *,
        ROUND(AVG(cnt) OVER (PARTITION BY cnt), 2) AS cnt_avg_cnt,
        ROUND(cnt / NULLIF(SUM(cnt) OVER (PARTITION BY cnt), 0), 4) AS cnt_share_of_cnt,
        LEAD(first_reported_at_hour, 2) OVER (PARTITION BY cnt, status ORDER BY first_reported_at_hour, claim_type, status) AS next2_first_reported_at_hour,
        LAG(first_reported_at_hour) OVER w_claim_type_by_stddev_avg_app AS prev_first_reported_at_hour,
        ROW_NUMBER() OVER (PARTITION BY claim_type ORDER BY stddev_avg_approved_amount_plus_claimed_amount, claim_type, status) AS stddev_avg_approved_amount_plus_claimed_amount_s
    FROM per_group
    WINDOW w_claim_type_by_stddev_avg_app AS (PARTITION BY claim_type ORDER BY stddev_avg_approved_amount_plus_claimed_amount, claim_type, status)
),
with_trends AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(cnt_avg_cnt - AVG(cnt_avg_cnt) OVER (PARTITION BY status, claim_type), 2) AS cnt_avg_cnt_vs_status_claim_type_avg,
        LAG(cnt_share_of_cnt) OVER (PARTITION BY cnt, status ORDER BY cnt_share_of_cnt DESC, claim_type, status) AS prev_cnt_share_of_cnt,
        ROUND(AVG(cnt_share_of_cnt) OVER (PARTITION BY cnt, status ORDER BY cnt_share_of_cnt DESC, claim_type, status ROWS BETWEEN 1 PRECEDING AND CURRENT ROW), 2) AS cnt_share_of_cnt_avg_2
    FROM metrics
),
ranked AS (
    -- Window metrics over several partitions
    SELECT
        *,
        SUM(cnt_share_of_cnt_avg_2) OVER (PARTITION BY status, stddev_avg_approved_amount_plus_claimed_amount_s) AS status_stddev_avg_approved_amount_plus_claimed_a,
        DENSE_RANK() OVER w_claim_type_cnt_by_cnt_avg_cn AS cnt_avg_cnt_vs_status_claim_type_avg_dense_rank
    FROM with_trends
    WINDOW w_claim_type_cnt_by_cnt_avg_cn AS (PARTITION BY claim_type, cnt ORDER BY cnt_avg_cnt_vs_status_claim_type_avg DESC, claim_type, status)
)
-- Subtotals with GROUPING SETS
SELECT
    claim_type,
    stddev_avg_approved_amount_plus_claimed_amount_s,
    GROUPING(claim_type) AS is_total_claim_type,
    ROUND(STDDEV(cnt), 2) AS stddev_cnt,
    SUM(stddev_avg_approved_amount_plus_claimed_amount) AS total_stddev_avg_approved_amount_plus_claimed_am
FROM ranked
GROUP BY GROUPING SETS ((claim_type, stddev_avg_approved_amount_plus_claimed_amount_s), (claim_type))
