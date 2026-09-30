from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("is_flagged")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .filter(F.col("amount_range") >= 46)
    .select("is_flagged", "amount_range", "merchant_values")
    .orderBy("merchant_values", "amount_range")
)
