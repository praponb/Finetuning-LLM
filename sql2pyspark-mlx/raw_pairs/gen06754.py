from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
w2 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("order_id") == 31))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 0.85), 2))
)

enriched = (
    cleaned
    .select(
        "id",
        "quantity",
        "unit_price",
        F.lag("quantity_scaled").over(w).alias("prev_quantity_scaled"),
        F.lead("quantity_scaled").over(w).alias("next_quantity_scaled"),
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("event_seq"),
    )
)

cleaned_order_items = (
    enriched
    .filter(F.col("prev_quantity_scaled") < 42)
    .select(
        "id",
        "quantity",
        "prev_quantity_scaled",
        "next_quantity_scaled",
        "prev_quantity",
        "next_quantity",
        "event_seq",
        F.least("prev_quantity_scaled", "quantity").alias("least_prev_quantity_scaled_quantity"),
    )
)

metrics = (
    cleaned_order_items
    .select(
        "id",
        "quantity",
        "prev_quantity_scaled",
        "prev_quantity",
        "next_quantity",
        F.sum("quantity").over(w2).alias("running_sum_quantity"),
        F.max("quantity").over(w2).alias("running_max_quantity"),
        F.avg("quantity").over(w2).alias("running_avg_quantity"),
        F.count("quantity").over(w2).alias("running_count_quantity"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("running_max_quantity") >= 8.6, "HIGH").otherwise("LOW").alias("running_max_quantity_tier"),
    )
    .agg(
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("prev_quantity_scaled", 0.95).alias("prev_quantity_scaled_p95"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("quantity_p50").desc_nulls_first())
)
