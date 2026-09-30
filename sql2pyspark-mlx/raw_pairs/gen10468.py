from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date")
w2 = Window.partitionBy("status").orderBy("row_num", "id")

valid = (
    df_subscriptions
    .filter(F.col("end_date") >= F.lit("2023-04-05").cast("date"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
        F.concat_ws("/", "plan", "status").alias("plan_status_key"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

valid_subscriptions = (
    ranked
    .filter(F.col("status").isin(["ACTIVE", "EXPIRED"]))
    .select(
        "*",
        F.trim("status").alias("clean_status"),
        F.upper("plan").alias("plan_upper"),
    )
)

result = (
    valid_subscriptions
    .withColumn("activity_month", F.date_trunc("MONTH", "end_date"))
    .rollup("activity_month")
    .agg(
        F.count_if(F.col("plan") == "basic").alias("basic_events"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping("activity_month").alias("gid"),
    )
)
