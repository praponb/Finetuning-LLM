from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1570)
    .select("id", "status", "amount")
    .orderBy("status", F.col("id").desc())
    .limit(3)
)
