from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.to_date("started_at"), "device").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy(F.to_date("started_at"), "country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.orderBy(F.col("started_at").desc(), "id")
w4 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

with_windows = (
    df_sessions
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("event_seq"),
        F.first("browser", ignorenulls=True).over(w4).alias("first_known_browser"),
    )
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "seq_no",
        "event_seq",
        "first_known_browser",
        F.when(F.col("browser").rlike("^(Chrome|Edge)$"), "GROUP_A").otherwise("GROUP_B").alias("browser_zone"),
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("event_seq").asc_nulls_last())
)
