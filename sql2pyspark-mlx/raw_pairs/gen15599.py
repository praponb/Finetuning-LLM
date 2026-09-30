from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("merchant").orderBy("is_shell", "id")

cleaned = (
    df_transactions
    .filter(F.col("amount").between(2700, 3800) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        F.when(F.col("merchant") == "Shell", 1).otherwise(0).alias("is_shell"),
        F.length("merchant").alias("merchant_length"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "account_id",
        "amount",
        "merchant",
        "is_shell",
        "merchant_length",
        F.lag("merchant_length").over(w_hist).alias("prev_merchant_length"),
        F.lead("is_shell").over(w_hist).alias("next_is_shell"),
        F.lag("is_shell").over(w_hist).alias("prev_is_shell"),
        F.row_number().over(w_hist).alias("seq_no"),
    )
)
