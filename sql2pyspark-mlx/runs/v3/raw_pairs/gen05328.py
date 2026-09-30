from pyspark.sql import functions as F

result = (
    df_customers
    .select("country")
    .distinct()
    .crossJoin(df_suppliers.select("city").distinct())
    .orderBy("country", "city")
)
