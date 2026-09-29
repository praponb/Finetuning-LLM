from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("start_date").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id")
w3 = Window.orderBy(F.col("plan_length").desc(), "id")
w4 = Window.partitionBy("plan", "customer_id")
w5 = Window.partitionBy(F.year("start_date"), "status").orderBy("plan_length", "id")
w6 = Window.partitionBy("status").orderBy(F.col("plan_length").desc_nulls_last())

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.concat_ws(",", "plan", "status").alias("plan_status_key"),
    )
)

ranked = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned_subscriptions = (
    ranked
    .filter(F.col("end_date") > F.lit("2023-10-05").cast("date"))
    .withColumn("latest_rank_rounded", F.round("latest_rank", 2))
    .withColumn("plan_length", F.length("plan"))
)

with_windows = (
    cleaned_subscriptions
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "latest_rank_rounded",
        "plan_length",
        F.row_number().over(w2).alias("row_seq"),
        F.ntile(4).over(w3).alias("plan_length_bucket"),
        F.max("plan_length").over(w4).alias("plan_max_plan_length"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "start_date",
        "end_date",
        "status",
        "latest_rank_rounded",
        "row_seq",
        "plan_length_bucket",
        "plan_max_plan_length",
        F.ntile(3).over(w5).alias("plan_length_bucket_2"),
        F.dense_rank().over(w6).alias("plan_length_dense_rank"),
    )
)
