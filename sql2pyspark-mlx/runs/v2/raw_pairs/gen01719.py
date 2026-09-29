from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") > 27) & (F.col("status") == "RETURNED"))
    .agg(
        F.sum("amount").alias("total_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").desc())
)
