from pyspark.sql import functions as F

base = df_transactions.filter(F.col("amount") < 1500)

agg = (
    base
    .groupBy("is_flagged")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = (
    agg
    .select("is_flagged", "txn_type_values")
    .orderBy(F.col("txn_type_values").desc(), "is_flagged")
    .limit(5)
)
