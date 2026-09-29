from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Consumer"]))
    .groupBy(
        F.when(F.col("city").isin(["Bangkok", "New York"]), "group_a").otherwise("other").alias("city_group"),
        "is_active",
    )
    .agg(F.count("*").alias("customer_count"))
    .orderBy("city_group", F.col("customer_count").desc(), "is_active")
    .limit(3)
)
