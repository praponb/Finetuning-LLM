from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", F.col("credit_limit")).otherwise(0)).alias("corporate_credit_limit"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
    )
    .orderBy("city", F.col("avg_credit_limit").desc())
)
