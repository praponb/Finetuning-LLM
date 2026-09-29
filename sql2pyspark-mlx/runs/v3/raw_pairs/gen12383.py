from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "balance",
        "customer_id",
        F.dense_rank().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 5)
    .select("id", "status", "balance", "customer_id")
)
