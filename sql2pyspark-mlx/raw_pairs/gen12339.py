from pyspark.sql import functions as F

base = (
    df_transactions
    .filter((F.col("txn_time") < "2025-03-01") & F.col("merchant").isin(["Amazon", "Grab", "Lazada"]))
)

agg = (
    base
    .groupBy(F.to_date("txn_time").alias("txn_day"))
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
)

result = (
    agg
    .select("txn_day", "merchant_list")
    .orderBy("txn_day")
    .limit(20)
)
