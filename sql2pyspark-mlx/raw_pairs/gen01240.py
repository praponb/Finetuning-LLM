from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment")
    .pivot("country", ["TH", "DE"])
    .agg(F.sum("age").alias("amount"), F.count(F.lit(1)).alias("item_count"))
)
