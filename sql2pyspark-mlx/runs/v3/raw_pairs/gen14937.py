from pyspark.sql import functions as F

src = df_order_items.filter(F.col("product_id").isNull())

unit_price_bucket_summary = (
    src
    .groupBy(
        F.when(F.col("unit_price") >= 400, "high").when(F.col("unit_price") >= 90, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(F.max("quantity").alias("max_quantity"))
)

result = unit_price_bucket_summary.select("unit_price_bucket", "max_quantity")
