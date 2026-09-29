from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("account_type").orderBy("balance")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.rank().over(w2).alias("rnk"),
    )
)
