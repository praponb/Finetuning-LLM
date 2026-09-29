from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("city", "region").orderBy("next_sqft", "id")
w4 = Window.partitionBy(F.year("opened_date"), "region").orderBy(F.col("event_seq").desc(), "id")
w_recent = Window.partitionBy("region").orderBy("prev_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_stores
    .filter(F.col("opened_date").between("2023-03-01", "2024-01-31"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.lead("sqft").over(w).alias("next_sqft"),
        F.row_number().over(w).alias("event_seq"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

scored = (
    with_last
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "prev_sqft",
        "next_sqft",
        "inventory_last_restock",
        "inventory_quantity",
        F.lead("inventory_quantity").over(w_ordered).alias("next_inventory_quantity"),
        F.lag("sqft").over(w_ordered).alias("prev_sqft_2"),
        F.lead("sqft").over(w_ordered).alias("next_sqft_2"),
        F.row_number().over(w_ordered).alias("event_seq_2"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.inventory_last_restock")), "left_anti")
)

metrics = (
    kept
    .select(
        "id",
        "region",
        "prev_sqft",
        "next_sqft",
        "prev_sqft_2",
        "next_sqft_2",
        "row_seq",
        F.max("prev_sqft_2").over(w_recent).alias("running_max_prev_sqft_2"),
        F.min("prev_sqft_2").over(w_recent).alias("running_min_prev_sqft_2"),
        F.avg("prev_sqft_2").over(w_recent).alias("running_avg_prev_sqft_2"),
        F.count("prev_sqft_2").over(w_recent).alias("running_count_prev_sqft_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "prev_sqft",
        "next_sqft",
        "prev_sqft_2",
        "next_sqft_2",
        "running_min_prev_sqft_2",
        "running_avg_prev_sqft_2",
        "running_count_prev_sqft_2",
    )
    .orderBy(F.col("prev_sqft_2").asc_nulls_first(), F.col("next_sqft_2").desc())
)
