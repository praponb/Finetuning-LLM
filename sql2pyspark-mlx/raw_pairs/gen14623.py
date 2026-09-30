from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock_period").asc_nulls_last(), "id")
w_part = Window.partitionBy("store_id", "product_id").orderBy("last_restock_period_2", "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("session_id").desc(), "id")
w6 = Window.partitionBy("store_id", "product_id").orderBy("last_restock_period_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-09-02")))
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
    .withColumn("last_restock_period_2", F.date_trunc("year", "last_restock"))
)

with_prev = base.withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "last_restock_period",
        "last_restock_period_2",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

deduped = (
    sessionized
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "last_restock_period_2",
        "session_id",
        "rank_in_group",
        F.lead("session_id").over(w_part).alias("next_session_id"),
        F.row_number().over(w_part).alias("seq_no"),
    )
)

deduped_inventory = (
    enriched
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

enriched_inventory = (
    deduped_inventory
    .select(
        "id",
        "last_restock",
        "session_id",
        "rank_in_group",
        "next_session_id",
        "seq_no",
        F.min("session_id").over(w6).alias("running_min_session_id"),
        F.avg("session_id").over(w6).alias("running_avg_session_id"),
        F.count("session_id").over(w6).alias("running_count_session_id"),
        F.max("session_id").over(w6).alias("running_max_session_id"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "session_id",
        "rank_in_group",
        "seq_no",
        "running_min_session_id",
        "running_max_session_id",
    )
)
