from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy("last_restock_period", "id")
w5 = Window.partitionBy("product_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("store_id_max_last_restock_day").desc(), "id")
w8 = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w9 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock_period").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-03-13").cast("date"))
    .withColumn("last_restock_period", F.date_trunc("MONTH", "last_restock"))
    .withColumn("last_restock_day", F.dayofmonth("last_restock"))
)

scored = (
    valid
    .withColumn("quantity_cume_dist", F.cume_dist().over(w))
    .withColumn("store_id_max_last_restock_day", F.max("last_restock_day").over(w2))
)

latest = (
    scored
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    latest
    .withColumn("is_new_session", F.when(F.col("last_restock_period") - F.lag("last_restock_period").over(w4) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_inventory = gaps.withColumn("session_id", F.sum("is_new_session").over(w5))

scored_inventory = (
    sessions_inventory
    .withColumn("last_restock_day_count_7d", F.count("last_restock_day").over(w6))
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("quantity_cume_dist_count_30d", F.count("quantity_cume_dist").over(w8))
)

ranked = (
    scored_inventory
    .withColumn("latest_rank", F.row_number().over(w9))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "quantity",
        "last_restock",
        "quantity_cume_dist",
        "session_id",
        "last_restock_day_count_7d",
        "event_seq",
        "quantity_cume_dist_count_30d",
    )
    .orderBy(F.col("quantity").desc(), "quantity_cume_dist")
)
