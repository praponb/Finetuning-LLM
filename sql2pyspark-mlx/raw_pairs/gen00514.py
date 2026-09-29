from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_plus_months", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy(F.year("last_restock_plus_months"), "store_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("last_restock_plus_months_year_max_quantity").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("last_restock_plus_months_year_max_quantity").desc_nulls_last())
w6 = Window.partitionBy("product_id").orderBy("running_quantity", "id")

base = (
    df_inventory
    .filter(F.col("last_restock").between("2023-05-01", "2025-10-29"))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 12))
)

metrics = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_plus_months",
        F.lead("quantity", 2, 0).over(w).alias("next_quantity"),
        F.sum("quantity").over(w2).alias("running_quantity"),
        F.max("quantity").over(w_ordered).alias("last_restock_plus_months_year_max_quantity"),
    )
)

latest = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "quantity",
        "next_quantity",
        "running_quantity",
        "last_restock_plus_months_year_max_quantity",
        F.dense_rank().over(w5).alias("last_restock_plus_months_year_max_quantity_dense_rank"),
        F.row_number().over(w6).alias("event_seq"),
    )
)

valid = (
    enriched
    .filter(F.col("next_quantity") == 81)
    .select(
        "id",
        "next_quantity",
        "last_restock_plus_months_year_max_quantity",
        "last_restock_plus_months_year_max_quantity_dense_rank",
        "event_seq",
        (F.col("quantity") * F.col("event_seq")).alias("quantity_times_event_seq"),
    )
)

result = (
    valid
    .withColumn("quantity_times_event_seq_tier", F.when(F.col("quantity_times_event_seq") >= 69, "LARGE").otherwise("SMALL"))
    .withColumn("event_seq_tier", F.when(F.col("event_seq") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quantity_times_event_seq_tier", "event_seq_tier"], ["quantity_times_event_seq_tier"], []],
        "quantity_times_event_seq_tier",
        "event_seq_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("quantity_times_event_seq", [0.25, 0.5, 0.75]).alias("quantity_times_event_seq_quartiles"),
    )
    .orderBy(F.col("quantity_times_event_seq_tier").desc())
)
