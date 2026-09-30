from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") < 220) & ~F.col("plan").isin(["enterprise", "standard", "basic"]))
    .select(
        "id",
        "customer_id",
        "plan",
        F.date_add("start_date", 11).alias("start_date_plus_11d"),
    )
)
