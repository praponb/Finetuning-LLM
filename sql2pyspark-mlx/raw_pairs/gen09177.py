from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")
w2 = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        "opened_date",
        F.first("status").over(w).alias("first_status"),
        F.rank().over(w2).alias("rnk"),
    )
)
