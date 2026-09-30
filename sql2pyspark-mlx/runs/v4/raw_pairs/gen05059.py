from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("start_date").desc())
w2 = Window.partitionBy("status").orderBy("start_date", "id")
w3 = Window.partitionBy("plan").orderBy("start_date_dayofweek", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("is_expired").desc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("is_expired").asc_nulls_last(), "id")
w6 = Window.partitionBy("status").orderBy(F.col("last_known_start_date_dayofweek").desc_nulls_last())

base = (
    df_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-05-01"))) & F.col("customer_id").isNotNull() & F.col("start_date").between("2025-01-01", "2025-03-20"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("status") == "EXPIRED", 1).otherwise(0).alias("is_expired"),
        F.length("status").alias("status_length"),
    )
)

ranked = (
    base
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

cleaned = (
    ranked
    .filter(F.col("end_date") >= "2023-10-29")
    .withColumn("plan_lower", F.lower("plan"))
    .withColumn("start_date_dayofweek", F.dayofweek("start_date"))
)

with_windows = (
    cleaned
    .withColumn("prev_status_length", F.lag("status_length", 1).over(w2))
    .withColumn("last_known_start_date_dayofweek", F.last("start_date_dayofweek", ignorenulls=True).over(w3))
    .withColumn("is_expired_dense_rank", F.dense_rank().over(w4))
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

deduped_subscriptions = (
    deduped
    .withColumn("rn", F.rank().over(w6))
    .filter(F.col("rn") == 1)
)

result = (
    deduped_subscriptions
    .groupBy("plan")
    .pivot("status", ["ACTIVE", "EXPIRED", "CANCELLED"])
    .agg(F.max("is_expired").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "plan",
        F.col("ACTIVE_amount").alias("active_amount"),
        F.col("ACTIVE_n").alias("active_n"),
        F.col("EXPIRED_amount").alias("expired_amount"),
        F.col("EXPIRED_n").alias("expired_n"),
        F.col("CANCELLED_amount").alias("cancelled_amount"),
        F.col("CANCELLED_n").alias("cancelled_n"),
    )
)
