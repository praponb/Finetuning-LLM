from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type")
w_hist = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        F.count("balance").over(w).alias("account_type_count_balance"),
        F.ntile(10).over(w_hist).alias("balance_bucket"),
    )
)
