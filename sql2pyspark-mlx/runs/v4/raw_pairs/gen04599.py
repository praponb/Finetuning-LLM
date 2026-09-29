from pyspark.sql import functions as F

base = (
    df_subscriptions
    .filter(F.col("customer_id").isNull() & (F.col("plan") == "premium"))
)

agg = (
    base
    .groupBy("status")
    .agg(F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"))
)

result = (
    agg
    .filter(F.col("basic_count") < 7)
    .select("status", "basic_count")
    .orderBy("basic_count")
)
