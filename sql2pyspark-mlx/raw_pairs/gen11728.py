from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("status_length").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy("rn", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("status").orderBy("monthly_fee", "id")

cleaned = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-05-20").cast("date")) & (F.col("end_date") >= "2025-06-01"))
    .withColumn("status_length", F.length("status"))
    .withColumn("status_plan_key", F.concat_ws(",", "status", "plan"))
)

latest = (
    cleaned
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.min("status_length").over(w3).alias("last3_min_status_length"),
        F.avg("status_length").over(w3).alias("last3_avg_status_length"),
        F.count("status_length").over(w3).alias("last3_count_status_length"),
        F.sum("status_length").over(w3).alias("last3_sum_status_length"),
        F.ntile(10).over(w4).alias("monthly_fee_bucket"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id").eqNullSafe(F.col("c.id")), "left")
    .select("s.*", F.col("c.country"))
)

result = (
    with_dims
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .cube(
        F.col("p.country").alias("customer_country"),
        F.col("a.country").alias("subscription_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("a.status_length")).alias("min_status_length"),
        F.grouping_id().alias("grouping_level"),
    )
)
