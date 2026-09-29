from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee")
w2 = Window.partitionBy("plan", "status")
w3 = Window.orderBy(F.col("days_start_date_to_end_date").asc_nulls_first())
w4 = Window.partitionBy("customer_id", "status")

base = (
    df_subscriptions
    .filter((F.col("end_date") >= F.lit("2023-10-02").cast("date")) & (F.col("status") != "EXPIRED"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.when(F.col("monthly_fee") >= 248, "High").when(F.col("monthly_fee") >= 72, "Medium").otherwise("Low").alias("monthly_fee_band"),
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
    )
)

remaining_keys = (
    base
    .select("plan")
    .exceptAll(
        df_subscriptions
        .filter(F.month("end_date").isin([2, 5, 7]))
        .select("plan")
    )
)

verified = (
    base
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("customer_id") == 18)
    .withColumn("months_since_start_date", (F.datediff(F.current_date(), "start_date") / 30))
    .withColumn("days_start_date_to_end_date", F.datediff("end_date", "start_date"))
)

enriched = (
    filtered
    .filter(F.col("customer_id").isin([15, 32]))
    .select(
        "*",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.count("months_since_start_date").over(w2).alias("plan_count_months_since_start_date"),
    )
)

enriched_subscriptions = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.start_date",
        "a.end_date",
        "a.monthly_fee",
        "a.status",
        "a.start_date_label",
        "a.months_since_start_date",
        "a.days_start_date_to_end_date",
        "a.monthly_fee_dense_rank",
        "a.plan_count_months_since_start_date",
        F.col("b.credit_limit"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "*",
        F.dense_rank().over(w3).alias("days_start_date_to_end_date_dense_rank"),
        F.sum("days_start_date_to_end_date").over(w4).alias("customer_id_sum_days_start_date_to_end_date"),
    )
)
