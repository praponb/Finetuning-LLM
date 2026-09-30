from pyspark.sql import functions as F

src = df_customers.filter(F.col("country") != "SG")

agg = (
    src
    .groupBy("is_active")
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
)

result = (
    agg
    .filter(F.col("min_credit_limit") < 11500)
    .orderBy(F.col("min_credit_limit").desc(), "is_active")
    .limit(1)
)
