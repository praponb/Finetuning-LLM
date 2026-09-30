select *, datediff(current_date(), opened_date) as days_since_opened_date, concat(store_name, ' ', city) as store_name_city from stores order by store_name;
