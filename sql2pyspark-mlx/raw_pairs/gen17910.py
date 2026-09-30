from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy(F.year("txn_time"), "account_id").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w3 = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_transactions
    .filter(F.col("txn_time") < "2023-10-01")
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        F.dense_rank().over(w_ordered).alias("amount_dense_rank"),
        F.row_number().over(w2).alias("event_seq"),
        F.last("merchant", ignorenulls=True).over(w3).alias("last_known_merchant"),
    )
)

result = (
    enriched
    .select(
        "id",
        "txn_type",
        "merchant",
        "is_flagged",
        "amount_dense_rank",
        "event_seq",
        "last_known_merchant",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "CORE").otherwise("EXTENDED").alias("txn_type_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
