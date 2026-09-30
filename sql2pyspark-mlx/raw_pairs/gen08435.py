from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

metrics = (
    df_accounts
    .filter(F.col("balance") <= 38000)
    .select(
        "id",
        "account_type",
        F.min("balance").over(w).alias("last4_min_balance"),
        F.count("balance").over(w).alias("last4_count_balance"),
        F.max("balance").over(w).alias("last4_max_balance"),
        F.sum("balance").over(w).alias("last4_sum_balance"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

result = (
    metrics
    .select(
        "id",
        "last4_min_balance",
        "last4_count_balance",
        "last4_max_balance",
        "last4_sum_balance",
    )
    .orderBy(F.col("last4_sum_balance").desc_nulls_first())
)
