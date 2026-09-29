from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").asc_nulls_first())
w_hist = Window.partitionBy("customer_id").orderBy("end_date", "id")
w4 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w5 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_dense_rank").desc_nulls_first(), "id")
w7 = Window.partitionBy("status").orderBy("prev_monthly_fee", "id")
w8 = Window.partitionBy("status").orderBy("monthly_fee_dense_rank_2", "id")

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2024-01-25").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "end_date",
        "monthly_fee",
        "status",
        F.concat_ws(",", "status", "plan").alias("status_plan_key"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.name"))
)

metrics = (
    joined
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.dense_rank().over(w2).alias("monthly_fee_dense_rank"),
        F.lead("monthly_fee", 1).over(w_hist).alias("next_monthly_fee"),
        F.dense_rank().over(w4).alias("monthly_fee_dense_rank_2"),
        F.avg("monthly_fee").over(w5).alias("monthly_fee_avg_2"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    ranked
    .withColumn("seq_no", F.row_number().over(w7))
    .withColumn("prev_prev_monthly_fee", F.lag("prev_monthly_fee", 1).over(w8))
)

summary = (
    scored
    .groupBy(F.date_trunc("year", "end_date").alias("year_start"))
    .agg(
        F.percentile_approx("prev_prev_monthly_fee", [0.25, 0.5, 0.75]).alias("prev_prev_monthly_fee_quartiles"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
    .filter(F.col("num_status") < 1)
)

result = (
    summary
    .select("year_start", "prev_prev_monthly_fee_quartiles", "num_status")
    .orderBy("num_status")
)
