from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w3 = Window.partitionBy(F.date_trunc("YEAR", "last_restock")).orderBy(F.col("last_restock").desc(), "id")
w4 = Window.orderBy(F.col("next_quantity").desc(), "id")
w5 = Window.orderBy(F.col("event_seq").desc(), "id")
w6 = Window.partitionBy(F.date_trunc("month", "last_restock"))
w7 = Window.orderBy("quantity")
w8 = Window.orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        F.rank().over(w).alias("quantity_rank"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

metrics_inventory = (
    metrics
    .select(
        "id",
        "quantity",
        "last_restock",
        "next_quantity",
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("event_seq_2"),
        F.avg("next_quantity").over(w6).alias("last_restock_period_avg_next_quantity"),
        F.dense_rank().over(w7).alias("quantity_dense_rank"),
        F.sum("event_seq").over(w8).alias("cumulative_event_seq"),
    )
)

matched_keys = (
    metrics_inventory
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("product_id") == 1)
        .select("id")
    )
)

kept_rows = (
    metrics_inventory
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("next_quantity") <= 88)
    .select(
        "quantity",
        "last_restock",
        "next_quantity",
        "last_restock_period_avg_next_quantity",
        "quantity_dense_rank",
        "cumulative_event_seq",
        F.greatest("row_seq", "quantity").alias("greatest_row_seq_quantity"),
        (F.col("quantity") - F.col("next_quantity")).alias("quantity_minus_next_quantity"),
    )
)

summary = (
    base
    .groupBy(
        F.when(F.col("greatest_row_seq_quantity") >= 74, "HIGH").otherwise("LOW").alias("greatest_row_seq_quantity_tier"),
    )
    .agg(
        F.percentile_approx("cumulative_event_seq", 0.9).alias("cumulative_event_seq_p90"),
        F.round(F.avg("quantity_minus_next_quantity"), 2).alias("avg_quantity_minus_next_quantity"),
    )
    .filter(F.col("avg_quantity_minus_next_quantity") >= 0)
)

result = (
    summary
    .select("greatest_row_seq_quantity_tier", "cumulative_event_seq_p90")
    .orderBy(F.col("greatest_row_seq_quantity_tier").desc())
    .limit(5)
)
