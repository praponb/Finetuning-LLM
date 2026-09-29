from pyspark.sql import functions as F

valid_order_items = (
    df_order_items
    .filter((F.col("quantity") < 6) & F.col("order_id").isin([8, 11, 35]) & F.col("unit_price").between(310, 330))
)

order_id_summary = (
    valid_order_items
    .groupBy(
        "order_id",
        F.when(F.col("unit_price") >= 340, "high").when(F.col("unit_price") >= 120, "mid").otherwise("low").alias("unit_price_bucket"),
    )
    .agg(
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
        F.count("*").alias("num_order_items"),
    )
)

result = (
    order_id_summary
    .filter(F.col("num_order_items") > 5)
    .select("order_id", "unit_price_bucket", "revenue", "num_order_items")
)
