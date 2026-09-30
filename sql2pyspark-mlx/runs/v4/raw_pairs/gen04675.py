from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.col("customer_id") == 6)
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .orderBy(F.col("account_type").desc())
)
