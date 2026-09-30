from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date_period", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("end_date") >= F.to_date(F.lit("2024-05-30"))) & F.col("monthly_fee").isNull())
    .select("*", F.concat_ws("-", "plan", "status").alias("plan_status_key"))
)

with_dims = (
    valid
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.name"), F.col("d.segment"))
)

valid_subscriptions = (
    with_dims
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "plan_status_key",
        "name",
        "segment",
        F.date_trunc("month", "start_date").alias("start_date_period"),
        F.coalesce("segment", F.lit("N/A")).alias("resolved_segment"),
    )
)

gaps = (
    valid_subscriptions
    .withColumn("new_group_flag", F.when(F.lag("start_date_period").over(w).isNull() | (F.col("start_date_period") > F.lag("start_date_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("session_no", F.sum("new_group_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "plan_status_key",
        "name",
        "segment",
        "start_date_period",
    )
    .orderBy(F.col("status").desc_nulls_first())
)
