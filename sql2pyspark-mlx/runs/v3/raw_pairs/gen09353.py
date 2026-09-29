from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("amount_range") < 64)
)
