from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Berlin"]))
    .select("is_active")
)
