from pyspark.sql import functions as F

src = df_transactions.filter(F.col("account_id").isin([16, 33]))

agg = (
    src
    .groupBy("merchant")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("merchant"))).alias("merchant_values"),
    )
)

result = (
    agg
    .select("merchant", "merchant_values")
    .orderBy("merchant_values")
)
