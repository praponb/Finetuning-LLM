from pyspark.sql import functions as F

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.start_date",
        "src.end_date",
        F.col("b.age"),
    )
)

result = (
    joined
    .groupBy(
        F.date_trunc("week", "start_date").alias("week_start"),
        F.when(F.col("age") >= 66, "HIGH").otherwise("LOW").alias("age_tier"),
        "customer_id",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("cnt_2"),
    )
    .filter(F.col("cnt_2") > 1)
    .orderBy(F.col("cnt").desc())
)
