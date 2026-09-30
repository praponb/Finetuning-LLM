from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([2, 3, 7]))
    .select("launch_date")
    .intersect(df_employees.filter(F.length("full_name") > 12).select("hire_date"))
    .orderBy("launch_date")
)
