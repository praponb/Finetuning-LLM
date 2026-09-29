with with_windows as (
    -- Running and moving aggregates
    select
        *,
        last_value(browser) ignore nulls over (partition by country order by started_at asc, id asc rows between unbounded preceding and current row) as last_known_browser,
        row_number() over (partition by browser order by started_at asc, id asc) as row_seq,
        row_number() over w_hist as seq_no
    from sessions
    window w_hist as (partition by year(started_at), browser order by started_at asc, id asc)
),
attributed as (
    -- Non-equi join on a validity window
    select
        src.*,
        sub.id as subscription_id,
        sub.plan,
        sub.monthly_fee
    from with_windows src
    left join subscriptions sub
        on src.customer_id = sub.customer_id
       and src.started_at >= sub.start_date
       and src.started_at < sub.end_date + interval '14' day
),
enriched as (
    -- Attach customers attributes
    select a.*, c.signup_date
    from attributed a
    join customers c
        on a.customer_id <=> c.id
),
kept_keys as (
    -- Set intersection on keys
    select id
    from enriched
    intersect all
    select id
    from sessions
    where customer_id = 7
),
verified as (
    -- Keep rows whose keys survive the set operation
    select s.*
    from enriched s
    join kept_keys k on s.id = k.id
)
-- Join a dimension and aggregate
select
    c.city,
    a.browser,
    count(*) as rows_total,
    max(a.row_seq) as max_row_seq,
    grouping_id() as grouping_level
from verified a
join customers c on a.customer_id = c.id
group by rollup (c.city, a.browser);
