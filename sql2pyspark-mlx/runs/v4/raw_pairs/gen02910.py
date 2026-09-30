from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w4 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("amount").between(1200, 3600))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_filled"),
    )
)

gaps = (
    cleaned
    .withColumn("is_new_burst", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_burst").over(w2))

with_gaps = (
    sessionized
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w3) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("island_id_2", F.sum("new_group_flag").over(w4))

matched_keys = (
    islands
    .select("account_id")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 3700)
        .select("account_id")
    )
)

kept_rows = (
    islands
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(
        F.when(F.col("island_id_2") >= 8, "HIGH").otherwise("LOW").alias("island_id_2_tier"),
    )
    .agg(
        F.sum("is_new_burst").alias("total_is_new_burst"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.size(F.collect_set("txn_type")).alias("num_txn_types"),
    )
)
