from pyspark.sql import functions as F

valid_transactions = (
    df_transactions
    .filter(F.col("is_flagged") & (F.col("amount") <= 2000) & ~F.col("is_flagged"))
)

agg = (
    valid_transactions
    .groupBy("merchant")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = agg
