from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance")
w2 = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("balance", "id")
w4 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "*",
        F.rank().over(w).alias("balance_rank"),
        F.ntile(5).over(w2).alias("balance_bucket"),
        F.lag("balance", 1).over(w3).alias("prev_balance"),
        F.lag("balance", 1).over(w4).alias("prev_balance_2"),
    )
)
