with joined as (
    -- Attach customers attributes
    select
        src.id,
        src.customer_id,
        src.browser,
        src.country,
        b.city,
        b.age,
        b.country as customer_country
    from sessions src
    left join customers b
        on src.customer_id = b.id
),
with_last as (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    select s.*, top1.page
    from joined s
    join lateral (
            select c.page
            from web_events c
            where c.session_id = s.id
            order by c.duration_sec desc, c.id desc
            limit 1
        ) top1 on true
),
filtered as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        customer_id,
        browser,
        city,
        page,
        length(city) as city_length,
        initcap(trim(customer_country)) as customer_country_title,
        coalesce(browser, 'N/A') as resolved_browser
    from with_last
    where city is not null
),
cleaned as (
    -- Basic cleansing and defaults
    select
        id,
        browser,
        city,
        page,
        customer_country_title,
        resolved_browser,
        coalesce(city_length, 0) as city_length_filled
    from filtered
    where city = 'Berlin'
),
base as (
    -- Basic cleansing and defaults
    select
        id,
        browser,
        customer_country_title,
        resolved_browser,
        case when page in ('/search', '/product') then 'Tier 1' else 'Other' end as page_group
    from cleaned
    where city is not null
),
totals as (
    -- Aggregate with conditional metrics
    select
        browser,
        count(distinct browser) as distinct_browsers,
        count(*) filter (where browser = 'Firefox') as firefox_rows,
        sort_array(collect_set(browser)) as browser_list,
        count(case when browser = 'Safari' then 1 end) as safari_count
    from base
    group by browser
)
select
    browser,
    distinct_browsers,
    firefox_rows,
    browser_list,
    safari_count,
    case when browser rlike '^(Chrome|Firefox)$' then 'OTHER_ZONE' when browser rlike '^(Safari|Edge)$' then 'CORE' else 'EXTENDED' end as browser_zone
from totals
order by distinct_browsers desc nulls last;
