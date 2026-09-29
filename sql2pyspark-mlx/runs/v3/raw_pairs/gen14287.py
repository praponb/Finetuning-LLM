from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "id",
        "status",
        "balance",
        F.dense_rank().over(w).alias("balance_dense_rank"),
    )
    .orderBy(F.col("id").desc())
)
