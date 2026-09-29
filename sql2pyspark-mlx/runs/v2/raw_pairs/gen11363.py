from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~(F.col("city").isNull() & (F.col("country") != "US")))
    .groupBy(
        "city",
        F.when(F.col("credit_limit") >= 17500, "high").when(F.col("credit_limit") >= 4100, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.sort_array(F.collect_set("country")).alias("country_list"))
    .orderBy("city", "credit_limit_bucket")
    .limit(5)
)
