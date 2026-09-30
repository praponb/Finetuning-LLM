from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("monthly_fee") > 100) & ~F.col("status").isin(["EXPIRED", "ACTIVE"])))
    .select(
        "monthly_fee",
        F.trim("plan").alias("clean_plan"),
        (F.datediff(F.current_date(), "end_date") / 365.25).alias("years_since_end_date"),
    )
    .orderBy("clean_plan")
)
