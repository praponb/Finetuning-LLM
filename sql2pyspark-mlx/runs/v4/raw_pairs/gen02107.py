from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .groupBy("is_flagged")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        F.sum(F.when(F.col("merchant") == "7-Eleven", 1).otherwise(0)).alias("7_eleven_count"),
    )
)
