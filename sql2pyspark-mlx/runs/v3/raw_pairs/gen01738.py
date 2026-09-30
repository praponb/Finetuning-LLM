from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("start_date") <= "2023-11-01") & F.col("start_date").between("2023-04-01", "2024-08-14") & F.col("monthly_fee").isNotNull())
    .groupBy("plan")
    .agg(F.countDistinct("monthly_fee").alias("unique_customers"))
    .filter(F.col("unique_customers") > 5)
)
