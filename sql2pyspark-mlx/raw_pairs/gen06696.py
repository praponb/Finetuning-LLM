from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([10, 13, 26, 35]))
    .select(
        "id",
        "balance",
        "opened_date",
        F.ntile(3).over(w).alias("balance_bucket"),
        F.ntile(10).over(w2).alias("balance_bucket_2"),
    )
)
