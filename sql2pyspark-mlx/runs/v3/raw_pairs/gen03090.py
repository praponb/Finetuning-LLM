from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select(
        "id",
        "status",
        "balance",
        "customer_id",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "status", "balance", "customer_id")
    .orderBy("id", "status")
)
