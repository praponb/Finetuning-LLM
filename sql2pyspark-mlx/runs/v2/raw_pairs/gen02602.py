from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "account_type",
        "balance",
        F.lag("opened_date", 2).over(w).alias("prev_opened_date"),
        F.percent_rank().over(w2).alias("balance_pct_rank"),
    )
)
