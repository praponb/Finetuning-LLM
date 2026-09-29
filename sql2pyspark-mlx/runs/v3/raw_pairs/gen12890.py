from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~((F.col("plan") == "basic") | (F.col("plan") == "standard")))
    .select(
        "customer_id",
        "plan",
        "end_date",
        F.datediff(F.current_date(), "end_date").alias("days_since_end_date"),
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
    .orderBy("end_date")
)
