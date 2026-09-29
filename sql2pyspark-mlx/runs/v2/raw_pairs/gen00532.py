from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-01-08", "2024-11-02"))
    .groupBy(F.year("end_date").alias("end_year"), "customer_id")
    .agg(
        F.min("end_date").alias("first_end_date"),
        F.round(((100 * F.sum(F.when(F.col("plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
    )
    .filter(F.col("pct_standard") > 38)
    .select("end_year", "customer_id", "first_end_date", "pct_standard")
)
