from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_last())

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.cume_dist().over(w).alias("balance_cume_dist"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
    )
)
