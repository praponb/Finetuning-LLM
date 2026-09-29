from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy(F.col("is_open").desc(), "id")
w2 = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_accounts
    .filter(F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
        F.coalesce("status", F.lit("NONE")).alias("status_filled"),
    )
)

result = (
    base
    .select(
        "id",
        "customer_id",
        "status",
        "is_open",
        "status_filled",
        F.row_number().over(w_ordered).alias("seq_no"),
        F.sum("balance").over(w2).alias("cumulative_balance"),
    )
)
