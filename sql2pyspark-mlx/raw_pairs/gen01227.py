from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("unit_price") * F.col("quantity")).alias("line_total"))
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "product_id", "unit_price", "line_total")
)

cleaned = (
    latest
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isin([1, 10, 23, 24]))
    .withColumn("line_total_times_unit_price", (F.col("line_total") * F.col("unit_price")))
)

metrics = (
    cleaned
    .withColumn("product_id_count_line_total_times_unit_price", F.count("line_total_times_unit_price").over(w2))
    .withColumn("prev_line_total_times_unit_price", F.lag("line_total_times_unit_price", 1).over(w3))
)

result = (
    metrics
    .withColumn("line_total_times_unit_price_tier", F.when(F.col("line_total_times_unit_price") >= 63, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["product_id", "line_total_times_unit_price_tier"], ["product_id"]],
        "product_id",
        "line_total_times_unit_price_tier",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("line_total", [0.25, 0.5, 0.75]).alias("line_total_quartiles"),
        F.sum("line_total").alias("total_line_total"),
        F.count("*").alias("row_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("line_total", [0.25, 0.5, 0.75]).alias("line_total_quartiles_2"),
        F.grouping("product_id").alias("grouping_id"),
    )
    .orderBy(F.col("row_count").desc())
)
