from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date")

result = (
    df_accounts
    .filter(F.col("account_type") == "LOAN")
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        "account_type",
        F.lead("balance", 2, 0).over(w).alias("next_balance"),
        F.percent_rank().over(w2).alias("opened_date_pct_rank"),
    )
)
