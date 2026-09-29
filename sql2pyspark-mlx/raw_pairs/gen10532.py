from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("started_at", "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id", "device").orderBy(F.col("island_id_2").desc(), "id")
w6 = Window.orderBy("country_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("island_id", "id")
w8 = Window.partitionBy("customer_id", "device").orderBy(F.col("cumulative_island_id_2").desc_nulls_last(), "id")
amount_by_customer_id = df_orders.groupBy("customer_id").agg(F.avg("amount").alias("avg_order_amount"))

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-30 07:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .select("*", F.length("country").alias("country_length"))
)

gaps = (
    valid
    .withColumn("is_new_session", F.when(F.col("started_at") - F.lag("started_at").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_sessions = gaps.withColumn("island_id", F.sum("is_new_session").over(w2))

with_dims = (
    sessions_sessions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.email"), F.col("b.name"))
)

flagged = (
    with_dims
    .withColumn("session_start_flag", F.when(F.col("started_at") - F.lag("started_at").over(w3) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("island_id_2", F.sum("session_start_flag").over(w4))

with_windows = (
    islands
    .filter(F.col("name").like("%smith%"))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        "is_new_session",
        "island_id",
        "email",
        "island_id_2",
        F.ntile(3).over(w5).alias("island_id_2_bucket"),
        F.sum("island_id_2").over(w6).alias("cumulative_island_id_2"),
        F.row_number().over(w7).alias("row_seq"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .join(amount_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "island_id",
        "island_id_2_bucket",
        "cumulative_island_id_2",
        "row_seq",
        "row_num",
        F.col("avg_order_amount"),
    )
    .orderBy(F.col("row_seq").desc_nulls_first())
)
