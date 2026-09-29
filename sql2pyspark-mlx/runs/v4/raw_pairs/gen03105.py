from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy("txn_time_dayofweek", "id")
w2 = Window.partitionBy("account_id", "txn_type").orderBy("txn_time_dayofweek", "id").rowsBetween(-2, -1)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "is_flagged",
        F.length("txn_type").alias("txn_type_length"),
        F.dayofweek("txn_time").alias("txn_time_dayofweek"),
        F.coalesce("merchant", F.lit("OTHER")).alias("merchant_clean"),
    )
)

result = (
    base
    .withColumn("prev_txn_type_length", F.lag("txn_type_length", 2).over(w))
    .withColumn("txn_type_length_avg_2_prior", F.avg("txn_type_length").over(w2))
)
