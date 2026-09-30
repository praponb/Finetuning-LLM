from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 7, "High").when(F.col("quantity") >= 3, "Medium").otherwise("Low").alias("quantity_level"),
        F.when(F.col("unit_price") >= 310, "High").when(F.col("unit_price") >= 190, "Medium").otherwise("Low").alias("unit_price_level"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    ranked
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.launch_date"), F.col("p.stock_qty"))
)

valid_order_items = (
    joined
    .filter(F.col("order_id") == 32)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price_level",
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)

result = (
    valid_order_items
    .groupBy("product_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count("*").alias("event_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids_2"),
        F.min("quantity").alias("min_quantity"),
    )
)
