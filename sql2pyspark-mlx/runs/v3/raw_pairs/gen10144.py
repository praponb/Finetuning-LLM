from pyspark.sql import functions as F

result = (
    df_customers
    .select("segment")
    .distinct()
    .crossJoin(df_suppliers.select("city").distinct())
    .orderBy("segment", "city")
)
