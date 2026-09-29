from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("balance")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        F.lag("balance", 1).over(w).alias("prev_balance"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("rnk").desc())
)
