from pyspark.sql import functions as F

result = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["country", "plan"], ["country"], []],
        F.col("p.country").alias("country"),
        F.col("e.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.monthly_fee")).alias("total_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
