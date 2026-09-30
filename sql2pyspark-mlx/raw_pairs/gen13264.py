from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "balance",
        "account_type",
        F.first("account_type").over(w).alias("first_account_type"),
    )
    .orderBy("status")
)
