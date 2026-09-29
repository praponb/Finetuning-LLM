from pyspark.sql import functions as F

base = (
    df_payments
    .filter(F.col("order_id").isin([11, 24, 29, 35]) & ~F.col("status").isin(["FAILED", "SUCCESS"]))
)

agg = (
    base
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
)

result = agg.select("status", "method_list")
