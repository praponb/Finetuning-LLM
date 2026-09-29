from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_last(), "id")

latest = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    latest
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        F.when(F.col("merchant").rlike("^(Starbucks|Amazon)$"), "OTHER_ZONE").when(F.col("merchant").rlike("^(Grab|7-Eleven)$"), "GROUP_A").otherwise("PRIORITY").alias("merchant_zone"),
    )
)
