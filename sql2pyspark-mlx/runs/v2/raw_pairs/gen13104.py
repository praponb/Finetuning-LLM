from pyspark.sql import functions as F

result = (
    df_transactions
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
    .orderBy(F.col("merchant_values").desc())
)
