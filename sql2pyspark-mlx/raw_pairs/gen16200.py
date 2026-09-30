from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time").desc_nulls_first())

latest = (
    df_transactions
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("txn_type").rlike("^(DEBIT|CREDIT)$"), "CORE").otherwise("PRIORITY").alias("txn_type_zone"),
    )
    .orderBy("txn_type", "merchant")
)
