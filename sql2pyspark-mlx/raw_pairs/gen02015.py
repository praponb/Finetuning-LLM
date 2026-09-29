from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("is_flagged_flag").desc_nulls_last(), "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id")
w3 = Window.partitionBy("merchant").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("last_known_is_flagged_flag_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_date(F.lit("2023-05-31"))) & F.col("merchant").isNotNull())
    .withColumn("is_flagged_flag", F.col("is_flagged").cast("int"))
    .withColumn("is_flagged_flag_2", F.col("is_flagged").cast("int"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("NONE")))
)

ranked = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

enriched = (
    ranked
    .select(
        "id",
        "txn_type",
        "is_flagged",
        "is_flagged_flag_2",
        "rn",
        F.lead("is_flagged_flag_2", 1, 0).over(w2).alias("next_is_flagged_flag_2"),
        F.last("is_flagged_flag_2", ignorenulls=True).over(w3).alias("last_known_is_flagged_flag_2"),
    )
)

cleaned = (
    enriched
    .filter(F.col("txn_type") == "DEBIT")
    .select(
        "id",
        "is_flagged",
        "is_flagged_flag_2",
        "rn",
        "next_is_flagged_flag_2",
        "last_known_is_flagged_flag_2",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

scored = (
    cleaned
    .filter(F.col("is_flagged"))
    .select(
        "*",
        F.avg("is_flagged_flag_2").over(w4).alias("running_avg_is_flagged_flag_2"),
        F.min("is_flagged_flag_2").over(w4).alias("running_min_is_flagged_flag_2"),
        F.sum("is_flagged_flag_2").over(w4).alias("running_sum_is_flagged_flag_2"),
        F.count("is_flagged_flag_2").over(w4).alias("running_count_is_flagged_flag_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "is_flagged",
        "is_flagged_flag_2",
        "rn",
        "next_is_flagged_flag_2",
        "is_flagged_flag",
        "running_avg_is_flagged_flag_2",
        "running_sum_is_flagged_flag_2",
    )
)
