from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("order_id").isNull())
    .withColumn("amount_int", F.col("amount").cast("int"))
)

payments_stats = (
    src
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sum(F.when(F.col("method") == "cash", F.col("amount")).otherwise(0)).alias("cash_amount"),
    )
)

result = (
    payments_stats
    .select("method", "method_list", "cash_amount")
    .orderBy(F.col("cash_amount").desc(), "method")
    .limit(1)
)
