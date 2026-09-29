from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .select("id", "channel")
)
