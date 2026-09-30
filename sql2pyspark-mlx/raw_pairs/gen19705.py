from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

flagged = (
    df_sessions
    .withColumn("is_new_session", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = (
    flagged
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_session").over(w2).alias("island_id"),
    )
)

result = (
    islands
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.when(F.col("country").rlike("^(JP|UK)$"), "GROUP_B").when(F.col("country").rlike("^(TH|US)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("country_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
)
