from pyspark.sql import functions as F

filtered = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 150)
    .withColumn("weeks_start_date_to_end_date", (F.datediff("end_date", "start_date") / 7))
)

result = (
    filtered
    .select("id", "customer_id", "monthly_fee", "weeks_start_date_to_end_date")
    .unpivot(
        ids=["id", "customer_id"],
        values=["monthly_fee", "weeks_start_date_to_end_date"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
)
