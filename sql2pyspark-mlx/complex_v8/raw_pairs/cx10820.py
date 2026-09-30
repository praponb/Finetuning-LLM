from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("holder_id", "status").orderBy(F.col("start_date").desc(), F.col("id").desc())

base_policys = (
    df_policies
    .filter(F.col("product").isNotNull())
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.date_format(F.col("start_date"), "MMM yyyy").alias("start_date_label"),
    )
)

latest_per_key = (
    base_policys
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

labeled = (
    latest_per_key
    .filter(F.col("start_date_label").isNotNull())
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "start_date_label",
        F.date_format(F.col("start_date"), "yyyy-MM-dd").alias("start_date_label_2"),
    )
)

grouped = (
    labeled
    .groupBy(F.date_trunc("month", "end_date").alias("end_date_month"))
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.min("end_date").alias("first_end_date"),
        F.min("start_date").alias("first_start_date"),
        F.count("*").alias("cnt"),
        F.round(F.avg("premium"), 2).alias("avg_premium"),
        F.sum("premium").alias("sum_premium_check"),
    )
    .filter(F.col("sum_premium_check") > 1051.9)
    .drop("sum_premium_check")
)

result = (
    grouped
    .filter(F.col("cnt") >= 3)
    .select(
        "end_date_month",
        "distinct_status",
        "cnt",
        "avg_premium",
    )
    .orderBy("end_date_month", F.col("avg_premium").desc_nulls_first())
    .limit(10)
)
