from pyspark.sql import functions as F

base = df_orders.filter(F.col("amount") < 1200)

agg = (
    base
    .groupBy("channel")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("channel") == "web", F.col("amount")).otherwise(0)).alias("web_amount"),
    )
)

result = (
    agg
    .filter(F.col("web_amount") < 44)
    .orderBy(F.col("web_amount").desc())
)
