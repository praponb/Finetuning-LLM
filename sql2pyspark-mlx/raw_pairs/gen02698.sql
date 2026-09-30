with latest as (
    -- Keep one row per key (latest first)
    select *, row_number() over (partition by account_type order by balance desc, id) as rank_in_group
    from accounts
    qualify rank_in_group = 1
),
enriched as (
    -- Attach customers attributes
    select
        e.id,
        e.customer_id,
        e.account_type,
        e.status,
        e.rank_in_group,
        p.country
    from latest e
    left join customers p
        on e.customer_id <=> p.id
),
joined as (
    -- Attach customers attributes
    select e.*, d.email
    from enriched e
    left join customers d
        on e.customer_id = d.id
    where d.credit_limit < 8100
),
scored as (
    -- Running and moving aggregates
    select *, row_number() over (partition by customer_id order by rank_in_group, id) as row_seq
    from joined
)
-- Join a dimension and aggregate
select
    dim.city,
    e.status,
    count(*) as cnt,
    max(e.row_seq) as max_row_seq
from scored e
inner join customers dim on e.customer_id = dim.id
group by rollup (dim.city, e.status);
