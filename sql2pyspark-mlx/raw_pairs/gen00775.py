from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy(F.col("quantity").desc_nulls_last())
w6 = Window.partitionBy("store_id").orderBy(F.col("quantity_dense_rank").desc(), "id")
w7 = Window.partitionBy(F.year("last_restock"), "product_id").orderBy("quantity_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("store_id_avg_quantity", "id")
w9 = Window.partitionBy(F.month("last_restock"), "store_id").orderBy("quantity_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("quantity_dense_rank", "id")
w11 = Window.partitionBy("product_id").orderBy(F.col("quantity_dense_rank").desc())
w12 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.partitionBy(F.month("last_restock"), "product_id").orderBy("cumulative_quantity_dense_rank", "id")

deduped = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.lead("quantity", 1).over(w3).alias("next_quantity"),
        F.avg("quantity").over(w4).alias("store_id_avg_quantity"),
        F.dense_rank().over(w5).alias("quantity_dense_rank"),
    )
)

deduped_inventory = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_windows = (
    deduped_inventory
    .withColumn("cumulative_quantity_dense_rank", F.sum("quantity_dense_rank").over(w7))
    .withColumn("next_quantity_2", F.lead("quantity", 1).over(w8))
    .withColumn("first_known_seq_no", F.first("seq_no", ignorenulls=True).over(w9))
)

metrics = (
    with_windows
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "seq_no",
        "next_quantity",
        "store_id_avg_quantity",
        "quantity_dense_rank",
        "next_quantity_2",
        "first_known_seq_no",
        F.ntile(3).over(w10).alias("quantity_dense_rank_bucket"),
        F.rank().over(w11).alias("quantity_dense_rank_rank"),
        F.last("seq_no", ignorenulls=True).over(w12).alias("last_known_seq_no"),
        F.lead("first_known_seq_no", 1).over(w13).alias("next_first_known_seq_no"),
    )
)

daily = (
    metrics
    .rollup(
        F.date_trunc("year", "last_restock").alias("year_start"),
        F.when(F.col("quantity_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("quantity_dense_rank_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("quantity_dense_rank"), 2).alias("avg_quantity_dense_rank"),
        F.round(F.avg("first_known_seq_no"), 2).alias("avg_first_known_seq_no"),
    )
)

result = daily.select("year_start", "quantity_bit_and", "avg_quantity_dense_rank")
