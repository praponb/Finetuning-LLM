select *, weekofyear(opened_date) as opened_date_weekofyear from stores where city = 'New York' and region = 'Central'
