select id, location, dept_name, initcap(trim(location)) as location_title from departments order by location_title;
