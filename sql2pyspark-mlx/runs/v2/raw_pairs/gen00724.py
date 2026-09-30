from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 13400, "high").when(F.col("credit_limit") >= 6300, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
    .orderBy(F.col("credit_limit_bucket").desc())
)
