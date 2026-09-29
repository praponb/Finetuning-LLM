from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("merchant").orderBy(F.col("next_amount").desc(), "id")
w4 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_last(), "id")
w6 = Window.partitionBy("merchant").orderBy(F.col("amount").desc_nulls_first(), "id")
w7 = Window.partitionBy("merchant").orderBy("session_no", "id")
w8 = Window.partitionBy("merchant").orderBy("amount")
w9 = Window.partitionBy("merchant").orderBy(F.col("amount").desc())

filtered = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-07-07 17:00:00").cast("timestamp"))
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.date_format("txn_time", "MMM yyyy").alias("txn_time_label"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.lead("amount", 1).over(w_grp).alias("next_amount"),
        F.max("amount").over(w2).alias("amount_max_90d"),
        F.lead("amount", 1).over(w_grp).alias("next_amount_2"),
    )
)

latest = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

with_gaps = (
    latest
    .withColumn("new_group_flag", F.when(F.col("txn_time") - F.lag("txn_time").over(w_grp) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_transactions = with_gaps.withColumn("session_no", F.sum("new_group_flag").over(w4))

ranked = (
    sessions_transactions
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

ranked_transactions = (
    ranked
    .withColumn("rn_2", F.row_number().over(w6))
    .filter(F.col("rn_2") == 1)
)

scored_transactions = (
    ranked_transactions
    .filter(F.col("new_group_flag") > 1)
    .select(
        "*",
        F.lead("next_amount", 1).over(w7).alias("next_next_amount"),
        F.cume_dist().over(w8).alias("amount_cume_dist"),
        F.dense_rank().over(w9).alias("amount_dense_rank"),
    )
)

result = (
    scored_transactions
    .select(
        "id",
        "merchant",
        "next_amount_2",
        F.col("next_amount").alias("NEXT_AMOUNT"),
        F.col("amount").alias("AMOUNT"),
    )
    .unpivot(
        ids=["id", "merchant"],
        values=["next_amount_2", "NEXT_AMOUNT", "AMOUNT"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
