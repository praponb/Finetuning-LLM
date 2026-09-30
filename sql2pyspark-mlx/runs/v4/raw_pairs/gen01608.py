from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").desc(), "id")
w_running = Window.partitionBy("product_id").orderBy(F.col("unit_price_scaled").asc_nulls_first())

filtered = (
    df_order_items
    .filter(F.col("order_id").isin([21, 29]))
    .select(
        "*",
        F.round((F.col("unit_price") / F.col("quantity")), 2).alias("line_total"),
    )
)

cleaned = (
    filtered
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round((F.col("unit_price") * 1.07), 2).alias("unit_price_scaled"))
)

cleaned_order_items = (
    cleaned
    .filter(F.col("order_id").isin([5, 40]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total",
        "unit_price_scaled",
        F.ceil("unit_price").alias("unit_price_whole"),
        F.col("quantity").cast("string").alias("quantity_str"),
    )
)

deduped = (
    cleaned_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.dense_rank().over(w_running).alias("unit_price_scaled_dense_rank"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    metrics
    .groupBy("order_id")
    .agg(
        F.round(F.avg("unit_price_scaled"), 2).alias("avg_unit_price_scaled"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
    )
)
