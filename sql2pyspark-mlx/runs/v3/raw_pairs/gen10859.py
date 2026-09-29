from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .filter(F.col("products.stock_qty") >= 195)
    .groupBy(
        F.when(F.year(F.col("products.launch_date")) < 2024, "before_2024").when(F.year(F.col("products.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        (F.max(F.col("order_items.unit_price")) - F.min(F.col("order_items.unit_price"))).alias("unit_price_range"),
    )
    .orderBy(F.col("unit_price_range").desc(), "launch_cohort")
    .limit(5)
)
