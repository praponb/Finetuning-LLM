from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("merchant")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.count("*").alias("num_transactions"),
    )
    .filter(F.col("num_transactions") < 8)
    .select("merchant", "merchant_values", "num_transactions")
    .orderBy(F.col("merchant").desc())
    .limit(1)
)
