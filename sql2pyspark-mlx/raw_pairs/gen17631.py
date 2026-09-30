from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "Consumer"]))
    .select("id", "age")
    .subtract(df_customers.filter(F.col("age").isNotNull()).select("id", "age"))
)
