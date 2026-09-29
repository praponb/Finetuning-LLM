from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("product_id") == 10) & (F.col("order_id").isin([1, 37]) | F.col("product_id").isNotNull()))
    .groupBy(
        "product_id",
        F.when(F.col("quantity") >= 7, "high").when(F.col("quantity") >= 3, "mid").otherwise("low").alias("quantity_bucket"),
    )
    .agg(F.max("unit_price").alias("max_unit_price"))
    .filter(F.col("max_unit_price") < 350)
)
