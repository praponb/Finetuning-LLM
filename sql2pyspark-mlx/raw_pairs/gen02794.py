from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("txn_time").desc(), "id")

with_gaps = (
    df_transactions
    .withColumn("session_start_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.sum("session_start_flag").over(w2).alias("session_id"),
    )
)

ranked = (
    islands
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "is_flagged")
)

cleaned = (
    ranked
    .filter(~F.col("is_flagged"))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
    )
)

filtered = (
    cleaned
    .filter(~F.col("is_flagged"))
    .select(
        "*",
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label_2"),
    )
)

cleaned_transactions = (
    filtered
    .filter(F.col("is_flagged"))
    .select("*", F.col("is_flagged").cast("int").alias("is_flagged_flag"))
)

valid = (
    cleaned_transactions
    .filter(F.col("is_flagged_flag") != 20)
    .select(
        "*",
        F.when(F.col("is_flagged_flag") >= 80, "High").when(F.col("is_flagged_flag") >= 32, "Medium").otherwise("Low").alias("is_flagged_flag_band"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag_2"),
    )
)

filtered_transactions = (
    valid
    .filter(F.col("is_flagged_flag_2") <= 79)
    .select(
        "id",
        "is_flagged_label",
        "is_flagged_label_2",
        "is_flagged_flag",
        "is_flagged_flag_band",
        "is_flagged_flag_2",
        F.col("is_flagged").cast("int").alias("is_flagged_flag_3"),
    )
)

result = (
    filtered_transactions
    .select(
        "id",
        "is_flagged_label",
        "is_flagged_label_2",
        "is_flagged_flag",
        "is_flagged_flag_2",
        "is_flagged_flag_3",
    )
    .orderBy(F.col("id").desc())
)
