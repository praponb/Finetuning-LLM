from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance")

result = (
    df_accounts
    .filter(F.month("opened_date").isin([1, 5, 10]))
    .select(
        "id",
        "account_type",
        "balance",
        "customer_id",
        F.dense_rank().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 5)
    .select("id", "account_type", "balance", "customer_id")
)
