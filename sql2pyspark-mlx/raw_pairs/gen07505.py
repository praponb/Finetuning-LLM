from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2023-07-06") & F.col("customer_id").isNotNull() & F.col("monthly_fee").isNull())
    .groupBy(
        F.when(F.col("status").isin(["ACTIVE", "CANCELLED"]), "core").otherwise("other").alias("status_group"),
        F.quarter("end_date").alias("end_quarter"),
    )
    .agg(F.min("end_date").alias("first_end_date"))
    .orderBy(F.col("first_end_date").desc())
)
