from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type", "merchant").orderBy("txn_time_day", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.year("txn_time"), "account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("txn_time"), "merchant").orderBy("amount", "id")
w_hist = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())

valid = (
    df_transactions
    .filter(F.col("is_flagged"))
    .withColumn("txn_time_day", F.dayofmonth("txn_time"))
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

metrics = (
    deduped
    .filter(F.col("txn_time") < "2023-11-13")
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.sum("amount").over(w2).alias("running_amount"),
        F.sum("txn_time_day").over(w3).alias("running_txn_time_day"),
        F.ntile(5).over(w4).alias("amount_bucket"),
        F.cume_dist().over(w_hist).alias("amount_cume_dist"),
    )
)

filtered = (
    metrics
    .filter(F.col("txn_time") >= F.to_date(F.lit("2023-06-10")))
    .select(
        "*",
        F.round((F.col("amount_cume_dist") / F.col("running_amount")), 2).alias("amount_cume_dist_per_running_amount"),
    )
)

result = (
    filtered
    .select(
        "id",
        "merchant",
        "amount",
        "amount_cume_dist_per_running_amount",
        "amount_cume_dist",
    )
    .unpivot(
        ids=["id", "merchant"],
        values=["amount", "amount_cume_dist_per_running_amount", "amount_cume_dist"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
