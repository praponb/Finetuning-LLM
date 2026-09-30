from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("greatest_amount_is_flagged_flag").asc_nulls_first())
w2 = Window.partitionBy("merchant")
w3 = Window.partitionBy("account_id")
w4 = Window.partitionBy("merchant").orderBy("greatest_amount_is_flagged_flag")
w5 = Window.partitionBy("merchant").orderBy(F.col("greatest_amount_is_flagged_flag_rank").desc())
w6 = Window.partitionBy("merchant").orderBy(F.col("greatest_amount_is_flagged_flag_cume_dist").desc_nulls_last())

filtered = (
    df_transactions
    .filter(~F.col("merchant").eqNullSafe(F.lit("Amazon")))
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

matched_keys = (
    filtered
    .select("id", "merchant")
    .intersectAll(
        df_transactions
        .filter(F.col("merchant") != "Grab")
        .select("id", "merchant")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.merchant") == F.col("k.merchant")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("txn_time") >= F.lit("2023-06-28 16:00:00").cast("timestamp"))
    .select(
        "account_id",
        "merchant",
        "is_flagged_flag",
        "resolved_merchant",
        F.greatest("amount", "is_flagged_flag").alias("greatest_amount_is_flagged_flag"),
        F.concat_ws("-", "txn_type", "merchant").alias("txn_type_merchant_key"),
    )
)

metrics = (
    base
    .select(
        "merchant",
        "resolved_merchant",
        F.rank().over(w).alias("greatest_amount_is_flagged_flag_rank"),
        F.max("is_flagged_flag").over(w2).alias("merchant_max_is_flagged_flag"),
        F.max("greatest_amount_is_flagged_flag").over(w3).alias("account_id_max_greatest_amount_is_flagged_flag"),
        F.cume_dist().over(w4).alias("greatest_amount_is_flagged_flag_cume_dist"),
        F.sum("is_flagged_flag").over(w2).alias("merchant_sum_is_flagged_flag"),
    )
)

base_transactions = (
    metrics
    .filter(F.col("merchant_max_is_flagged_flag").between(21, 38))
    .select(
        "merchant",
        "resolved_merchant",
        "greatest_amount_is_flagged_flag_rank",
        "greatest_amount_is_flagged_flag_cume_dist",
        F.round((F.col("merchant_max_is_flagged_flag") * 0.9), 2).alias("merchant_max_is_flagged_flag_scaled"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

scored = (
    base_transactions
    .withColumn("greatest_amount_is_flagged_flag_rank_dense_rank", F.dense_rank().over(w5))
    .withColumn("greatest_amount_is_flagged_flag_rank_share", F.round((F.col("greatest_amount_is_flagged_flag_rank") / F.sum("greatest_amount_is_flagged_flag_rank").over(w2)), 4))
    .withColumn("greatest_amount_is_flagged_flag_cume_dist_cume_dist", F.cume_dist().over(w6))
)

result = (
    scored
    .rollup("merchant")
    .agg(
        F.max("merchant_max_is_flagged_flag_scaled").alias("max_merchant_max_is_flagged_flag_scaled"),
        F.round(F.avg("greatest_amount_is_flagged_flag_cume_dist_cume_dist"), 2).alias("avg_greatest_amount_is_flagged_flag_cume_dist_cume_dist"),
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_rows"),
        F.grouping("merchant").alias("grouping_id"),
    )
    .filter(F.col("starbucks_rows") >= 1)
    .orderBy(F.col("merchant").asc_nulls_first())
)
