from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age", "id")
w2 = Window.partitionBy(F.month("end_date"), "status").orderBy("monthly_fee", "id")
w3 = Window.partitionBy(F.year("end_date"), "plan").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id").orderBy("age", "id")

filtered = (
    df_subscriptions
    .filter(F.col("start_date") > F.lit("2023-09-23").cast("date"))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

joined = (
    filtered
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "inner")
    .filter(F.col("b.country").isin(["UK", "TH", "US"]))
    .select("s.*", F.col("b.age"), F.col("b.city"))
)

with_windows = (
    joined
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        "age",
        "city",
        F.ntile(3).over(w).alias("age_bucket"),
        F.lead("age", 2, 0).over(w2).alias("next_age"),
        F.sum("monthly_fee").over(w3).alias("cumulative_monthly_fee"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.sum("age").over(w4).alias("running_sum_age"),
        F.max("age").over(w4).alias("running_max_age"),
        F.avg("age").over(w4).alias("running_avg_age"),
    )
)

result = (
    metrics
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "city",
        "running_max_age",
        F.lag("running_max_age").over(w_recent).alias("prev_running_max_age"),
        F.lead("running_max_age").over(w_recent).alias("next_running_max_age"),
        F.lag("age").over(w_recent).alias("prev_age"),
        F.row_number().over(w_recent).alias("row_seq"),
    )
)
