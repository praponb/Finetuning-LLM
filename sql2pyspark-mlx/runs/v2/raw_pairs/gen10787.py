from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([23, 29, 36]))
    .groupBy(
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(
        (F.max("quantity") - F.min("quantity")).alias("quantity_range"),
        F.count("*").alias("num_order_items"),
    )
)
