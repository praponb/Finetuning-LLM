from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("line_total_2", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("line_total_2_times_unit_price", "id").rowsBetween(-5, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("line_total_2")
w4 = Window.partitionBy("product_id").orderBy(F.col("adjusted_line_total_2_avg_5").asc_nulls_first())
w5 = Window.partitionBy("product_id").orderBy("adjusted_line_total_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
        (F.col("unit_price") + F.col("quantity")).alias("line_total_2"),
    )
)

filtered_order_items = (
    filtered
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([7, 8, 34]))
    .withColumn("adjusted_line_total_2", F.round((F.col("line_total_2") * 100), 2))
    .withColumn("line_total_2_times_unit_price", (F.col("line_total_2") * F.col("unit_price")))
)

with_windows = (
    filtered_order_items
    .select(
        "id",
        "product_id",
        "line_total_2",
        "adjusted_line_total_2",
        "line_total_2_times_unit_price",
        F.avg("line_total_2").over(w).alias("line_total_2_avg_5"),
        F.avg("adjusted_line_total_2").over(w2).alias("adjusted_line_total_2_avg_5"),
    )
)

with_windows_order_items = (
    with_windows
    .filter(F.col("line_total_2").between(44, 51))
    .select(
        "id",
        "product_id",
        "adjusted_line_total_2",
        "line_total_2_times_unit_price",
        "line_total_2_avg_5",
        F.rank().over(w3).alias("line_total_2_rank"),
        F.dense_rank().over(w4).alias("adjusted_line_total_2_avg_5_dense_rank"),
        F.first("line_total_2", ignorenulls=True).over(w5).alias("first_known_line_total_2"),
    )
)

result = (
    with_windows_order_items
    .withColumn("line_total_2_times_unit_price_tier", F.when(F.col("line_total_2_times_unit_price") >= 88, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["line_total_2_times_unit_price_tier", "product_id"], ["line_total_2_times_unit_price_tier"]],
        "line_total_2_times_unit_price_tier",
        "product_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("adjusted_line_total_2_avg_5_dense_rank"), 2).alias("avg_adjusted_line_total_2_avg_5_dense_rank"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("line_total_2_times_unit_price_tier").desc())
)
