from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity_rounded", "id")
w_ordered = Window.partitionBy("product_id").orderBy(F.col("days_since_last_restock").desc(), "id")
w3 = Window.orderBy("days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w5 = Window.partitionBy("store_id").orderBy("days_since_last_restock_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock").between("2025-05-01", "2025-10-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
        F.round("quantity", 1).alias("quantity_rounded"),
    )
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "days_since_last_restock",
        "quantity_rounded",
    )
)

valid_inventory = (
    latest
    .filter(F.col("days_since_last_restock") != 60)
    .withColumn("days_since_last_restock_rounded", F.round("days_since_last_restock", 2))
)

metrics = (
    valid_inventory
    .select(
        "id",
        "quantity",
        "days_since_last_restock",
        "quantity_rounded",
        F.row_number().over(w_ordered).alias("event_seq"),
        F.first("days_since_last_restock_rounded", ignorenulls=True).over(w3).alias("first_known_days_since_last_restock_rounded"),
        F.row_number().over(w4).alias("row_seq"),
        F.sum("days_since_last_restock_rounded").over(w5).alias("cumulative_days_since_last_restock_rounded"),
    )
)

result = (
    metrics
    .select(
        "id",
        "quantity",
        "days_since_last_restock",
        "quantity_rounded",
        "event_seq",
        "first_known_days_since_last_restock_rounded",
        "row_seq",
        "cumulative_days_since_last_restock_rounded",
    )
    .orderBy(F.col("quantity_rounded").desc_nulls_last())
)
