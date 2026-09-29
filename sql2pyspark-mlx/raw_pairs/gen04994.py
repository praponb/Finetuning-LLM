from pyspark.sql import functions as F

result = (
    df_customers
    .select("city")
    .distinct()
    .crossJoin(df_employees.select("level").distinct())
    .orderBy("city", "level")
)
