select page, count(distinct duration_sec) as unique_pages, count(*) as total_count, max(duration_sec) - min(duration_sec) as duration_sec_range from web_events group by page;
