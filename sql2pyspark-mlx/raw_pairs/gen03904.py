from pyspark.sql import functions as F

src = df_orders.filter(F.col("status") == "COMPLETED")

summary = (
    src
    .groupBy("channel")
    .agg(F.count("amount").alias("amount_count"))
)

result = (
    summary
    .select("channel", "amount_count")
    .orderBy(F.col("amount_count").desc())
)
