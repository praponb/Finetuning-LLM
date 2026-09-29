from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "city")
    .agg(
        F.sum(F.when(F.col("country") == "JP", F.col("credit_limit")).otherwise(0)).alias("jp_credit_limit"),
    )
    .filter(F.col("jp_credit_limit") < 74)
    .select("segment", "city", "jp_credit_limit")
    .orderBy("city", "segment")
    .limit(5)
)
