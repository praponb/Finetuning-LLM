from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_last(), "id")

base = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-07-09 13:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
    )
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
