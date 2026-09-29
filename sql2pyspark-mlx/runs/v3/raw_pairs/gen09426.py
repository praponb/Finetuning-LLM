from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("status") == "EXPIRED") | F.col("customer_id").isin([16, 17, 38])))
    .select(
        "id",
        "status",
        "start_date",
        "customer_id",
        F.date_format("end_date", "MMM yyyy").alias("end_date_label"),
    )
    .orderBy(F.col("customer_id").desc())
)
