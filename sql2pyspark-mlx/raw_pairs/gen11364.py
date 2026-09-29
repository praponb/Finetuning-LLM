from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "customer_id").orderBy("started_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.min("monthly_fee").alias("min_subscription_monthly_fee"))

enriched = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.first("country", ignorenulls=True).over(w2).alias("first_known_country"),
    )
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "event_seq",
        "first_known_country",
        F.when(F.col("country").rlike("^(US|SG)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|JP)$"), "CORE").otherwise("PRIORITY").alias("country_zone"),
        F.col("min_subscription_monthly_fee"),
    )
    .orderBy(F.col("started_at").desc_nulls_last())
)
