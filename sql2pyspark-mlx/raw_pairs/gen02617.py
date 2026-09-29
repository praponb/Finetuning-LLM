from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

deduped = (
    df_transactions
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    deduped
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged",
        F.when(F.col("txn_type").rlike("^(CREDIT|DEBIT)$"), "EXTENDED").when(F.col("txn_type").rlike("^(REFUND|FEE)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("txn_type_zone"),
    )
    .orderBy("amount", "txn_type")
)
