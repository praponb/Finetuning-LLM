from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w4 = Window.partitionBy("product_id").orderBy("prev_quantity", "id")
w5 = Window.partitionBy("store_id", "product_id").orderBy(F.col("row_seq").desc_nulls_last(), "id")

latest = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest
    .withColumn("next_quantity", F.lead("quantity").over(w2))
    .withColumn("prev_quantity", F.lag("quantity").over(w2))
    .withColumn("seq_no", F.row_number().over(w2))
)

enriched = (
    with_windows
    .filter(F.col("rn") >= 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "rn",
        "prev_quantity",
        F.lead("next_quantity").over(w3).alias("next_next_quantity"),
        F.lead("prev_quantity").over(w3).alias("next_prev_quantity"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(10).over(w4).alias("prev_quantity_bucket"),
    )
)

latest_inventory = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "prev_quantity",
        "next_prev_quantity",
        "row_seq",
    )
)
