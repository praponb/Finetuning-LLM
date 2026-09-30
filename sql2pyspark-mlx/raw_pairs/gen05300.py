from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") < 4200)
    .drop("total_amount")
    .orderBy("status")
)
