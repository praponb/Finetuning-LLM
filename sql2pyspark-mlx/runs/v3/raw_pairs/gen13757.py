from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "order_id",
        F.when(F.col("quantity") >= 8, "high").when(F.col("quantity") >= 4, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("unit_price").alias("max_unit_price"))
    .filter(F.col("max_unit_price") > 140)
)
