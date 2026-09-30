select customer_id, started_at, initcap(trim(browser)) as browser_title
from sessions
where started_at >= '2025-01-14';
