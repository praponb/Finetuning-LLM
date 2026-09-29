from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("quantity_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock_quarter", "id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-06-28").cast("date")) & F.col("last_restock").between("2024-05-04", "2025-02-20"))
    .select(
        "*",
        F.round("quantity", 1).alias("quantity_rounded"),
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_rounded",
        "last_restock_quarter",
        F.avg("quantity_rounded").over(w_recent).alias("running_avg_quantity_rounded"),
        F.sum("quantity_rounded").over(w_recent).alias("running_sum_quantity_rounded"),
        F.count("quantity_rounded").over(w_recent).alias("running_count_quantity_rounded"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
