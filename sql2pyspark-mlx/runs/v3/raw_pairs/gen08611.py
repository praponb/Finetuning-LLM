from pyspark.sql import functions as F

src = df_order_items.filter(F.col("order_id").isin([13, 18]))

order_id_summary = (
    src
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.count("quantity").alias("quantity_count"))
)

result = order_id_summary.orderBy("quantity_bucket")
