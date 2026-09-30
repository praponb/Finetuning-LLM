from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("opened_date").asc_nulls_last(), "id")
w3 = Window.partitionBy("region").orderBy(F.col("quantity").desc())
w4 = Window.partitionBy("region", "city").orderBy("sqft", "id")
w5 = Window.orderBy("quantity_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("quantity_percent_rank").desc())
w_recent = Window.orderBy("running_max_quantity_percent_rank", "id")
w_part = Window.orderBy("row_seq", "id")
w9 = Window.orderBy("running_min_quantity_percent_rank", "id")

with_last = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

ranked = (
    with_last
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

scored = (
    ranked
    .select(
        "id",
        "sqft",
        "quantity",
        F.percent_rank().over(w3).alias("quantity_percent_rank"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

enriched = (
    scored
    .withColumn("running_max_quantity_percent_rank", F.max("quantity_percent_rank").over(w5))
    .withColumn("running_count_quantity_percent_rank", F.count("quantity_percent_rank").over(w5))
    .withColumn("running_min_quantity_percent_rank", F.min("quantity_percent_rank").over(w5))
    .withColumn("quantity_percent_rank_percent_rank", F.percent_rank().over(w6))
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 2, 4])), "left_semi")
)

with_windows = (
    eligible
    .withColumn("next_quantity_percent_rank", F.lead("quantity_percent_rank").over(w_recent))
    .withColumn("prev_quantity_percent_rank", F.lag("quantity_percent_rank").over(w_recent))
    .withColumn("prev_running_max_quantity_percent_rank", F.lag("running_max_quantity_percent_rank").over(w_recent))
    .withColumn("row_seq_2", F.row_number().over(w_recent))
)

result = (
    with_windows
    .select(
        "id",
        "sqft",
        "quantity_percent_rank",
        "running_max_quantity_percent_rank",
        "running_count_quantity_percent_rank",
        "running_min_quantity_percent_rank",
        "quantity_percent_rank_percent_rank",
        "next_quantity_percent_rank",
        "prev_quantity_percent_rank",
        F.lag("row_seq").over(w_part).alias("prev_row_seq"),
        F.lead("row_seq").over(w_part).alias("next_row_seq"),
        F.lead("quantity_percent_rank").over(w_part).alias("next_quantity_percent_rank_2"),
        F.lag("quantity_percent_rank").over(w_part).alias("prev_quantity_percent_rank_2"),
        F.row_number().over(w_part).alias("event_seq"),
        F.ntile(4).over(w9).alias("running_min_quantity_percent_rank_bucket"),
    )
)
