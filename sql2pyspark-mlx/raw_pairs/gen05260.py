from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.orderBy("quantity", "id")
w3 = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("quantity_cume_dist").desc(), "id")
w5 = Window.orderBy("last_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("quantity_cume_dist")

metrics = (
    df_order_items
    .filter(F.col("quantity") == 7)
    .select(
        "id",
        "order_id",
        "unit_price",
        F.cume_dist().over(w).alias("quantity_cume_dist"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
    )
)

base = (
    metrics
    .filter(F.col("order_id").isin([16, 27, 30]))
    .select(
        "id",
        "quantity_cume_dist",
        "last_known_quantity",
        F.when(F.col("unit_price") >= 330, "High").when(F.col("unit_price") >= 130, "Medium").otherwise("Low").alias("unit_price_tier"),
    )
)

enriched = (
    base
    .select(
        "id",
        "quantity_cume_dist",
        F.row_number().over(w4).alias("event_seq"),
        F.first("last_known_quantity", ignorenulls=True).over(w5).alias("first_known_last_known_quantity"),
        F.cume_dist().over(w6).alias("quantity_cume_dist_cume_dist"),
        F.sum("quantity_cume_dist").over(w5).alias("cumulative_quantity_cume_dist"),
    )
)

base_order_items = (
    enriched
    .filter(F.col("event_seq") == 3)
    .select(
        "id",
        "quantity_cume_dist",
        "event_seq",
        "first_known_last_known_quantity",
        "quantity_cume_dist_cume_dist",
        "cumulative_quantity_cume_dist",
        (F.col("cumulative_quantity_cume_dist") - F.col("event_seq")).alias("cumulative_quantity_cume_dist_minus_event_seq"),
    )
)

valid = (
    base_order_items
    .filter(F.col("cumulative_quantity_cume_dist") > 4)
    .withColumn("quantity_cume_dist_plus_event_seq", (F.col("quantity_cume_dist") + F.col("event_seq")))
)

result = (
    valid
    .groupBy(
        F.when(F.col("cumulative_quantity_cume_dist") >= 4, "LARGE").otherwise("SMALL").alias("cumulative_quantity_cume_dist_tier"),
        F.when(F.col("cumulative_quantity_cume_dist_minus_event_seq") >= 86, "HIGH").otherwise("LOW").alias("cumulative_quantity_cume_dist_minus_event_seq_tier"),
    )
    .agg(
        F.max("cumulative_quantity_cume_dist").alias("max_cumulative_quantity_cume_dist"),
        F.count("*").alias("event_count"),
    )
)
