from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy(F.date_trunc("week", "last_restock"), "product_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.date_trunc("YEAR", "last_restock"), "product_id").orderBy(F.col("last_restock").desc(), "id")
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id", "store_id").orderBy("first_known_quantity", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("row_seq").desc(), "id")
w7 = Window.partitionBy("product_id").orderBy("row_seq", "id")
w8 = Window.partitionBy("store_id", "product_id").orderBy("quantity_whole", "id")
w9 = Window.orderBy("first_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("product_id").orderBy("row_seq", "id").rowsBetween(-6, -1)
w11 = Window.partitionBy("store_id").orderBy("next_first_known_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("quantity") == 223)
    .select("*", F.floor("quantity").alias("quantity_whole"))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity_whole",
        F.first("quantity", ignorenulls=True).over(w2).alias("first_known_quantity"),
        F.row_number().over(w3).alias("row_seq"),
        F.min("quantity_whole").over(w4).alias("product_id_min_quantity_whole"),
    )
)

metrics_inventory = (
    metrics
    .filter(F.col("product_id").isin([2, 4, 8, 10]))
    .withColumn("prev_first_known_quantity", F.lag("first_known_quantity", 2).over(w5))
    .withColumn("row_seq_bucket", F.ntile(5).over(w6))
    .withColumn("next_first_known_quantity", F.lead("first_known_quantity", 1).over(w7))
)

result = (
    metrics_inventory
    .select(
        "id",
        "store_id",
        "quantity_whole",
        "first_known_quantity",
        "prev_first_known_quantity",
        F.row_number().over(w8).alias("event_seq"),
        F.sum("next_first_known_quantity").over(w9).alias("running_next_first_known_quantity"),
        F.avg("row_seq").over(w10).alias("row_seq_avg_6_prior"),
        F.sum("row_seq").over(w11).alias("cumulative_row_seq"),
    )
)
