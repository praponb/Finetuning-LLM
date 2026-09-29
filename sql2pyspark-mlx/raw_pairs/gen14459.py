from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w_grp = Window.partitionBy("plan", "status").orderBy("rn", "id")
w3 = Window.partitionBy("status").orderBy("start_date", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("customer_id", "subscription_plan").orderBy(F.col("monthly_fee_avg_3").asc_nulls_last(), "id", "subscription_id")

deduped = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_windows = (
    deduped
    .filter(F.col("monthly_fee") < 150)
    .select(
        "*",
        F.lag("monthly_fee").over(w_grp).alias("prev_monthly_fee"),
        F.row_number().over(w_grp).alias("seq_no"),
        F.avg("monthly_fee").over(w3).alias("monthly_fee_avg_3"),
    )
)

with_window = (
    with_windows
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.end_date") >= F.col("sub.start_date")) & (F.col("src.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 7 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

ranked = (
    with_window
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    ranked
    .select(
        "id",
        "subscription_id",
        "plan",
        F.col("prev_monthly_fee").alias("PREV_MONTHLY_FEE_VAL"),
        F.col("monthly_fee").alias("MONTHLY_FEE"),
    )
    .unpivot(
        ids=["id", "subscription_id", "plan"],
        values=["PREV_MONTHLY_FEE_VAL", "MONTHLY_FEE"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
