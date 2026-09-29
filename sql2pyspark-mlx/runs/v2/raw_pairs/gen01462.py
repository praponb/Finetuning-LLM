from pyspark.sql import functions as F

recent_transactions = (
    df_transactions
    .filter((F.col("amount") < 2600) & F.hour("txn_time").between(12, 19) & ~F.col("merchant").isin(["Amazon", "7-Eleven"]))
)

summary = (
    recent_transactions
    .groupBy("merchant")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("txn_type"))).alias("txn_type_values"),
    )
)

result = summary.orderBy(F.col("merchant").desc(), F.col("txn_type_values").desc())
