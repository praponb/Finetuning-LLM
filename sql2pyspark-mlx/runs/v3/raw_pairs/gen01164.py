from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .select(
        "id",
        "status",
        "account_type",
        "balance",
        F.percent_rank().over(w).alias("balance_pct_rank"),
    )
)
