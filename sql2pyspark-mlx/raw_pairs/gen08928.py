from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock_quarter").asc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w3 = Window.partitionBy(F.year("last_restock_plus_months"), "store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy("last_restock_plus_months", "id")
w_ordered = Window.partitionBy("store_id").orderBy("quantity_bucket", "id")
w7 = Window.partitionBy("store_id").orderBy("next_quantity", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-06").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.quarter("last_restock").alias("last_restock_quarter"),
    )
)

cleaned = (
    valid
    .filter(F.col("last_restock") >= F.lit("2023-03-23").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        "last_restock_quarter",
        (F.col("quantity") * F.col("last_restock_quarter")).alias("quantity_times_last_restock_quarter"),
        F.add_months("last_restock", 6).alias("last_restock_plus_months"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "quantity", "last_restock_plus_months")
)

ranked = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

with_windows = (
    ranked
    .filter(F.col("rn") >= 1)
    .select(
        "id",
        "store_id",
        F.ntile(10).over(w3).alias("quantity_bucket"),
        F.max("quantity").over(w4).alias("store_id_max_quantity"),
        F.lead("quantity", 1, 0).over(w5).alias("next_quantity"),
    )
)

result = (
    with_windows
    .withColumn("prev_next_quantity", F.lag("next_quantity").over(w_ordered))
    .withColumn("next_next_quantity", F.lead("next_quantity").over(w_ordered))
    .withColumn("next_quantity_bucket", F.lead("quantity_bucket").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w7))
)
