from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2023-06-21").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.round((F.col("monthly_fee") * 1.07), 2).alias("adjusted_monthly_fee"),
    )
)

result = (
    base
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .cube(F.col("p.country").alias("country"), F.col("src.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.adjusted_monthly_fee")).alias("max_adjusted_monthly_fee"),
        F.max(F.col("src.monthly_fee")).alias("max_monthly_fee"),
        F.grouping_id().alias("grouping_level"),
    )
)
