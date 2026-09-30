from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("city").isin(["Singapore", "Bangkok"]), "priority").otherwise("other").alias("city_group"),
        "country",
        "is_active",
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count("credit_limit").alias("credit_limit_count"),
        F.avg("credit_limit").alias("avg_credit_limit"),
    )
    .orderBy("city_group")
)
