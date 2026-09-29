from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

deduped = (
    df_subscriptions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("end_date").desc())
)
