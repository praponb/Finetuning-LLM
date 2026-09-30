from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("last_restock", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("quantity") >= 86)
    .select(
        "id",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.round((F.col("quantity") * 1.15), 2).alias("adjusted_quantity"),
    )
)

base = (
    cleaned
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-05-22"))) & F.col("last_restock").between("2024-09-01", "2025-03-14"))
    .select(
        "*",
        F.coalesce("adjusted_quantity", F.lit(0)).alias("adjusted_quantity_filled"),
        F.coalesce("adjusted_quantity", F.lit(0)).alias("adjusted_quantity_filled_2"),
    )
)

base_inventory = (
    base
    .filter((F.col("last_restock") >= F.lit("2023-05-09").cast("date")) & (F.col("adjusted_quantity_filled") <= 54))
    .select(
        "*",
        (F.col("adjusted_quantity") - F.col("adjusted_quantity_filled")).alias("adjusted_quantity_minus_adjusted_quantity_filled"),
    )
)

enriched = (
    base_inventory
    .filter(F.col("adjusted_quantity_filled") >= 64)
    .select(
        "*",
        F.count("adjusted_quantity_filled").over(w_running).alias("last7_count_adjusted_quantity_filled"),
        F.min("adjusted_quantity_filled").over(w_running).alias("last7_min_adjusted_quantity_filled"),
        F.max("adjusted_quantity_filled").over(w_running).alias("last7_max_adjusted_quantity_filled"),
        F.avg("adjusted_quantity_filled").over(w_running).alias("last7_avg_adjusted_quantity_filled"),
    )
)

cleaned_inventory = (
    enriched
    .filter(F.col("adjusted_quantity_minus_adjusted_quantity_filled").isNotNull())
    .select(
        "id",
        "last_restock",
        "adjusted_quantity",
        "adjusted_quantity_filled",
        "adjusted_quantity_filled_2",
        "adjusted_quantity_minus_adjusted_quantity_filled",
        "last7_count_adjusted_quantity_filled",
        "last7_avg_adjusted_quantity_filled",
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)

result = (
    cleaned_inventory
    .select(
        "id",
        "last_restock",
        "adjusted_quantity",
        "adjusted_quantity_filled_2",
        "last7_count_adjusted_quantity_filled",
        "last7_avg_adjusted_quantity_filled",
        "last_restock_plus_months",
    )
    .orderBy(F.col("last7_avg_adjusted_quantity_filled").desc())
)
