from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w3 = Window.partitionBy("merchant").orderBy(F.col("is_flagged_flag").desc())
w4 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-07-29").cast("date")) & F.col("amount").between(1800, 3900))
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

result = (
    valid
    .withColumn("next_amount", F.lead("amount", 1).over(w))
    .withColumn("amount_dense_rank", F.dense_rank().over(w2))
    .withColumn("is_flagged_flag_cume_dist", F.cume_dist().over(w3))
    .withColumn("amount_bucket", F.ntile(5).over(w4))
    .withColumn("last_known_is_flagged_flag", F.last("is_flagged_flag", ignorenulls=True).over(w5))
)
