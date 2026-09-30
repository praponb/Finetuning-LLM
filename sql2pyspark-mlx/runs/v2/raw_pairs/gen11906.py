from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("account_id", "merchant")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)
