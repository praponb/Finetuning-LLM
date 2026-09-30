from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("unit_price").asc_nulls_first())
w_ordered = Window.partitionBy("product_id").orderBy("unit_price", "id")
w_hist = Window.partitionBy("product_id").orderBy(F.col("price").desc(), "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 220))
    .select("*", F.col("quantity").cast("string").alias("quantity_num"))
)

joined = (
    valid
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.product_name").like("%Yog%"))
    .select(
        "src.id",
        "src.product_id",
        "src.unit_price",
        "src.quantity_num",
        F.col("p.launch_date"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"), F.col("p.stock_qty"))
)

enriched_order_items = (
    enriched
    .withColumn("unit_price_percent_rank", F.percent_rank().over(w))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_hist))
)

grouped = (
    enriched_order_items
    .groupBy(F.when(F.col("seq_no") >= 8, "HIGH").otherwise("LOW").alias("seq_no_tier"))
    .agg(
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.round(F.avg("unit_price_percent_rank"), 2).alias("avg_unit_price_percent_rank"),
    )
    .filter(F.col("avg_stock_qty") >= 0)
)

result = grouped.select("seq_no_tier")
