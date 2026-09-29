from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.month("store_sale_sale_date"), "region").orderBy("store_sale_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("store_sale_sale_date", "id")
w4 = Window.partitionBy(F.year("store_sale_sale_date"), "city").orderBy(F.col("store_sale_sale_date").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w6 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("quantity").desc(), "id")

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-05-17").cast("date"))
    .select("id", "city", "region", F.lower("store_name").alias("store_name_lower"))
)

with_last = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

enriched = (
    with_last
    .select(
        "id",
        "city",
        "store_name_lower",
        F.last("region", ignorenulls=True).over(w2).alias("last_known_region"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.last_known_region",
        "s.event_seq",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.amount") >= 1240), "left_anti")
)

metrics = (
    kept
    .withColumn("running_max_event_seq", F.max("event_seq").over(w6))
    .withColumn("running_count_event_seq", F.count("event_seq").over(w6))
    .withColumn("running_sum_event_seq", F.sum("event_seq").over(w6))
    .withColumn("seq_no", F.row_number().over(w7))
)

remaining_keys = (
    metrics
    .select("id")
    .intersect(
        df_stores
        .filter(F.col("store_name").like("Ol%"))
        .select("id")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    verified
    .cube(F.date_trunc("YEAR", "inventory_last_restock").alias("year_start"))
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.sum("running_count_event_seq").alias("total_running_count_event_seq"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.sum("running_max_event_seq").alias("total_running_max_event_seq"),
    )
)

result = (
    totals
    .select(
        "year_start",
        "avg_quantity",
        "total_running_count_event_seq",
        "event_seq_quartiles",
        "quantity_p95",
        "total_running_max_event_seq",
    )
    .orderBy(F.col("quantity_p95").desc_nulls_last())
)
