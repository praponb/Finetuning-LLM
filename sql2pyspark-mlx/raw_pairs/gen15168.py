from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_ordered = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_first())
w3 = Window.partitionBy("country", "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("city").orderBy("credit_limit", "id")

filtered = (
    df_customers
    .filter(F.col("credit_limit").between(13800, 14200))
    .withColumn("name_lower", F.lower("name"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "name_lower",
        F.avg("credit_limit").over(w).alias("city_avg_credit_limit"),
        F.dense_rank().over(w_ordered).alias("credit_limit_dense_rank"),
        F.sum("age").over(w3).alias("running_age"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

scored = (
    with_last
    .withColumn("next_running_age", F.lead("running_age").over(w_grp))
    .withColumn("prev_credit_limit_dense_rank", F.lag("credit_limit_dense_rank").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

result = (
    scored
    .groupBy(
        F.when(F.col("prev_credit_limit_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("prev_credit_limit_dense_rank_tier"),
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("running_age", [0.25, 0.5, 0.75]).alias("running_age_quartiles"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy("activity_month")
)
