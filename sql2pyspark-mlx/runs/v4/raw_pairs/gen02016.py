from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "account_id").orderBy("amount", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("amount_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id", "txn_type").orderBy("amount_filled", "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-13").cast("date")) & F.col("amount").between(2900, 3900))
    .select(
        "*",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

scored = (
    cleaned
    .withColumn("next_amount", F.lead("amount", 1).over(w))
    .withColumn("amount_sum_30d", F.sum("amount").over(w2))
    .withColumn("cumulative_amount_filled", F.sum("amount_filled").over(w3))
    .withColumn("cumulative_amount", F.sum("amount").over(w4))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .cube(
        F.when(F.col("cumulative_amount_filled") >= 375, "LARGE").otherwise("SMALL").alias("cumulative_amount_filled_tier"),
        F.when(F.col("amount") >= 3600, "LARGE").otherwise("SMALL").alias("amount_tier"),
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.sum(F.when(F.col("txn_type") == "FEE", F.col("amount_sum_30d")).otherwise(0)).alias("fee_amount_sum_30d"),
        F.sum(F.when(F.col("merchant") == "Grab", F.col("amount_sum_30d"))).alias("grab_total_amount_sum_30d"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
        F.max("txn_time").alias("last_txn_time"),
        F.percentile_approx("next_amount", 0.95).alias("next_amount_p95"),
    )
    .filter(F.col("grab_total_amount_sum_30d") >= 0)
    .orderBy(F.col("fee_amount_sum_30d").desc_nulls_last())
)
