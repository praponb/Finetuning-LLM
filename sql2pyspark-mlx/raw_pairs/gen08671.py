from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").asc_nulls_first(), "id")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & F.col("is_flagged"))
    .select(
        "*",
        (F.datediff(F.current_date(), "txn_time") / 365.25).alias("years_since_txn_time"),
    )
)

result = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "txn_type", "txn_time", "merchant")
)
