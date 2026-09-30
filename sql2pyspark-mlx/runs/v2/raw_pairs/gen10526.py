from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance")
w2 = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "customer_id",
        "opened_date",
        F.dense_rank().over(w).alias("balance_dense_rank"),
        F.lag("opened_date", 2).over(w2).alias("prev_opened_date"),
    )
)
