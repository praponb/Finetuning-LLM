from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_first())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id", "account_type").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.rank().over(w).alias("balance_rank"),
        F.row_number().over(w2).alias("seq_no"),
        F.lag("balance", 2).over(w3).alias("prev_balance"),
    )
)
