from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("start_date", "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

scored = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 70)
    .select(
        "*",
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    scored
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "seq_no",
        F.when(F.col("plan").rlike("^(standard|enterprise)$"), "OTHER_ZONE").when(F.col("plan").rlike("^(basic|premium)$"), "PRIORITY").otherwise("CORE").alias("plan_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
)
