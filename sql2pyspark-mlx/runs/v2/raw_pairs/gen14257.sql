select id, location from departments where budget < 2916000 intersect select id, location from departments where location = 'London'
