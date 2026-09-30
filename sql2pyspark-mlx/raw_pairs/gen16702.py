from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
w_running = Window.partitionBy(F.year("last_restock"), "product_id").orderBy(F.col("quantity").asc_nulls_last())
w_recent = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("last_restock")
w6 = Window.partitionBy("product_id", "store_id").orderBy("quantity_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.month("last_restock"), "product_id").orderBy(F.col("quantity_dense_rank").desc(), "id")
w8 = Window.orderBy("quantity_bucket", "id")
w9 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, -1)

deduped = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "row_num",
        F.dense_rank().over(w_running).alias("quantity_dense_rank"),
        F.percent_rank().over(w_recent).alias("quantity_percent_rank"),
        F.ntile(10).over(w4).alias("quantity_bucket"),
    )
)

deduped_inventory = (
    enriched
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped_inventory
    .select(
        "*",
        F.first("quantity", ignorenulls=True).over(w6).alias("first_known_quantity"),
        F.ntile(5).over(w7).alias("quantity_dense_rank_bucket"),
        F.row_number().over(w8).alias("seq_no"),
        F.avg("quantity").over(w9).alias("quantity_avg_3_prior"),
    )
)

joined = (
    scored
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.quantity",
        "s.last_restock",
        "s.quantity_percent_rank",
        "s.quantity_bucket",
        "s.latest_rank",
        "s.first_known_quantity",
        "s.seq_no",
        "s.quantity_avg_3_prior",
        F.col("p.region"),
        F.col("p.store_name"),
    )
)

result = (
    joined
    .select(
        "id",
        "last_restock",
        "quantity_percent_rank",
        "latest_rank",
        "first_known_quantity",
        "seq_no",
    )
    .orderBy(F.col("seq_no").desc(), F.col("quantity_percent_rank").desc())
)
