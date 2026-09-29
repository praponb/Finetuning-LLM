from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("least_amount_max_30d_amount").desc())
w_hist = Window.partitionBy("account_id").orderBy(F.col("amount_rounded").asc_nulls_first())
w5 = Window.partitionBy(F.year("txn_time"), "txn_type").orderBy("amount_rounded", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy(F.to_date("txn_time"), "account_id").orderBy("amount_avg_6_prior", "id")
w7 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

valid = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & ~F.col("txn_type").isin(["DEBIT", "REFUND", "FEE"]))
    .select(
        "*",
        F.concat_ws("-", "merchant", "txn_type").alias("merchant_txn_type_key"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.sum("amount").over(w).alias("running_amount"),
        F.max("amount").over(w2).alias("amount_max_30d"),
    )
)

base = (
    scored
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        "merchant_txn_type_key",
        "amount_max_30d",
        F.least("amount_max_30d", "amount").alias("least_amount_max_30d_amount"),
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("OTHER")).alias("resolved_merchant"),
    )
)

latest = (
    base
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant_txn_type_key",
        "amount_rounded",
        "resolved_merchant",
    )
)

scored_transactions = (
    latest
    .filter(F.col("amount") > 1500)
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("amount_rounded_cume_dist"),
        F.avg("amount").over(w5).alias("amount_avg_6_prior"),
    )
)

metrics = (
    scored_transactions
    .select(
        "id",
        "amount",
        "merchant_txn_type_key",
        "amount_rounded",
        "resolved_merchant",
        F.ntile(4).over(w6).alias("amount_avg_6_prior_bucket"),
        F.count("amount_rounded_cume_dist").over(w7).alias("amount_rounded_cume_dist_count_90d"),
    )
)

cleaned = (
    metrics
    .filter(F.col("amount_rounded") >= 40)
    .select(
        "*",
        F.when(F.col("amount_avg_6_prior_bucket") >= 8, "High").when(F.col("amount_avg_6_prior_bucket") >= 3, "Medium").otherwise("Low").alias("amount_avg_6_prior_bucket_tier"),
        F.when(F.col("amount") >= 3800, "High").when(F.col("amount") >= 1300, "Medium").otherwise("Low").alias("amount_tier"),
    )
)

result = (
    cleaned
    .select("id", "amount", F.col("amount_rounded").alias("AMOUNT_ROUNDED_VAL"))
    .unpivot(
        ids=["id"],
        values=["amount", "AMOUNT_ROUNDED_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
