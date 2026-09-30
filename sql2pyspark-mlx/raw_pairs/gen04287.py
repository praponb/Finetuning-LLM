from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "plan").orderBy("subscription_id", "id", "subscription_id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("plan")
w3 = Window.partitionBy("customer_id").orderBy("subscription_id", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("status").orderBy(F.col("last3_min_monthly_fee").asc_nulls_first(), "id", "subscription_id")
w_ordered = Window.partitionBy(F.year("end_date"), "status")
w6 = Window.partitionBy("status", "plan").orderBy("last3_avg_monthly_fee", "id", "subscription_id").rowsBetween(-2, Window.currentRow)

base = (
    df_subscriptions
    .filter(F.col("monthly_fee") > 90)
    .select(
        "*",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.when(F.col("plan") == "standard", 1).otherwise(0).alias("is_standard"),
    )
)

matched = (
    base
    .alias("e")
    .join(df_subscriptions.alias("sub"), (F.col("e.customer_id") == F.col("sub.customer_id")) & (F.col("e.start_date") >= F.col("sub.start_date")) & (F.col("e.start_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "inner")
    .select(
        "e.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

scored = (
    matched
    .filter(F.col("subscription_monthly_fee").between(210, 220))
    .select(
        "*",
        F.count("monthly_fee").over(w).alias("last3_count_monthly_fee"),
        F.min("monthly_fee").over(w).alias("last3_min_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("last3_avg_monthly_fee"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.age"), F.col("c.credit_limit"))
)

enriched = (
    joined
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "is_standard",
        "subscription_id",
        "subscription_monthly_fee",
        "last3_count_monthly_fee",
        "last3_min_monthly_fee",
        "last3_avg_monthly_fee",
        "credit_limit",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.sum("last3_count_monthly_fee").over(w3).alias("cumulative_last3_count_monthly_fee"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

enriched_subscriptions = (
    deduped
    .select(
        "id",
        "status",
        "subscription_id",
        "last3_count_monthly_fee",
        "last3_avg_monthly_fee",
        "credit_limit",
        "cumulative_last3_count_monthly_fee",
        F.sum("cumulative_last3_count_monthly_fee").over(w_ordered).alias("end_date_year_sum_cumulative_last3_count_monthly_fee"),
        F.avg("subscription_id").over(w6).alias("subscription_id_avg_2"),
    )
)

result = (
    enriched_subscriptions
    .select("id", "subscription_id", "status", "last3_avg_monthly_fee", "credit_limit")
    .unpivot(
        ids=["id", "subscription_id", "status"],
        values=["last3_avg_monthly_fee", "credit_limit"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
