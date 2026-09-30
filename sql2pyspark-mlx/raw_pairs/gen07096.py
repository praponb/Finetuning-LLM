from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Chiang Mai", "New York"]))
    .groupBy(
        "country",
        F.when(F.col("age") >= 67, "high").when(F.col("age") >= 40, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.avg("age").alias("avg_age"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)
