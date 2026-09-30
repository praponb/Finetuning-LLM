select budget, location from departments where (location in ('Singapore', 'Berlin') and budget <= 1235000) or budget is not null order by 1 desc, location;
