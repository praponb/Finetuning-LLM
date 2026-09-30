from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc())

result = (
    df_accounts
    .filter(F.year("opened_date") >= 2025)
    .select(
        "id",
        "account_type",
        "opened_date",
        "balance",
        F.rank().over(w).alias("rnk"),
        F.rank().over(w2).alias("rnk_2"),
    )
)
