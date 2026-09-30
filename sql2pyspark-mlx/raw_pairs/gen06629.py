from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w_grp = Window.orderBy("last_restock", "id")
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("row_seq_minus_last_known_quantity", "id")
w5 = Window.orderBy(F.col("row_seq_minus_last_known_quantity").desc(), "id")
w6 = Window.orderBy("row_seq_minus_last_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("last_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("row_seq_2", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-07-17").cast("date"))
    .select(
        "*",
        (F.datediff(F.current_date(), "last_restock") / 7).alias("weeks_since_last_restock"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "quantity", "last_restock")
)

with_windows = (
    deduped
    .select(
        "id",
        "quantity",
        F.lag("quantity").over(w_grp).alias("prev_quantity"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("prev_quantity") < 198)
    .select(
        "*",
        (F.col("row_seq") - F.col("last_known_quantity")).alias("row_seq_minus_last_known_quantity"),
    )
)

metrics = (
    cleaned
    .filter(F.col("row_seq_minus_last_known_quantity").isNotNull())
    .select(
        "*",
        F.lead("prev_quantity").over(w_recent).alias("next_prev_quantity"),
        F.lag("prev_quantity").over(w_recent).alias("prev_prev_quantity"),
        F.lead("row_seq_minus_last_known_quantity").over(w_recent).alias("next_row_seq_minus_last_known_quantity"),
        F.row_number().over(w_recent).alias("row_seq_2"),
    )
)

metrics_inventory = (
    metrics
    .select(
        "id",
        "quantity",
        "row_seq",
        "last_known_quantity",
        "row_seq_minus_last_known_quantity",
        "next_prev_quantity",
        "next_row_seq_minus_last_known_quantity",
        F.row_number().over(w5).alias("event_seq"),
        F.first("row_seq_2", ignorenulls=True).over(w6).alias("first_known_row_seq_2"),
        F.first("prev_quantity", ignorenulls=True).over(w_running).alias("first_known_prev_quantity"),
        F.lead("last_known_quantity", 1, 0).over(w8).alias("next_last_known_quantity"),
        F.sum("row_seq_minus_last_known_quantity").over(w_running).alias("running_row_seq_minus_last_known_quantity"),
    )
)

cleaned_inventory = (
    metrics_inventory
    .filter(F.col("last_known_quantity") == 177)
    .select(
        "id",
        "quantity",
        "row_seq",
        "row_seq_minus_last_known_quantity",
        "next_prev_quantity",
        "first_known_row_seq_2",
        "first_known_prev_quantity",
        "next_last_known_quantity",
        "running_row_seq_minus_last_known_quantity",
        F.col("next_prev_quantity").cast("double").alias("next_prev_quantity_num"),
    )
)

result = (
    cleaned_inventory
    .select(
        "id",
        "quantity",
        "row_seq_minus_last_known_quantity",
        "first_known_row_seq_2",
        "first_known_prev_quantity",
        "next_last_known_quantity",
        "running_row_seq_minus_last_known_quantity",
    )
)
