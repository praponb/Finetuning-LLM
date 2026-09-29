from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("account_id", "merchant")
w2 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy("txn_type")

scored = (
    df_transactions
    .filter(F.col("amount") > 1900)
    .withColumn("account_id_count_amount", F.count("amount").over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w3)), 4))
)

result = (
    scored
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "account_id_count_amount",
        "row_seq",
    )
    .orderBy(F.col("txn_time").desc())
)
