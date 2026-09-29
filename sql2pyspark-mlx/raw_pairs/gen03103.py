from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock_period").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy(F.date_trunc("MONTH", "last_restock_period"), "store_id").orderBy(F.col("session_id").desc())
w6 = Window.partitionBy("store_id").orderBy(F.col("session_id").desc())
w7 = Window.partitionBy("product_id").orderBy(F.col("last_restock_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.orderBy("quantity", "id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-12")))
    .select("*", F.date_trunc("year", "last_restock").alias("last_restock_period"))
)

flagged = (
    filtered
    .withColumn("is_new_session", F.when(F.lag("last_restock_period").over(w).isNull() | (F.col("last_restock_period") > F.lag("last_restock_period").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = (
    flagged
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock_period",
        F.sum("is_new_session").over(w2).alias("session_id"),
    )
)

deduped = (
    sessionized
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .filter(F.col("last_restock_period") < "2024-05-01")
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("session_id_cume_dist", F.cume_dist().over(w5))
    .withColumn("session_id_percent_rank", F.percent_rank().over(w6))
    .withColumn("quantity_avg_7d", F.avg("quantity").over(w7))
    .withColumn("quantity_bucket", F.ntile(10).over(w8))
)

result = (
    with_windows
    .select(
        "id",
        "quantity",
        "last_restock_period",
        "session_id",
        "latest_rank",
        "event_seq",
        "session_id_cume_dist",
        "session_id_percent_rank",
        "quantity_avg_7d",
    )
)
