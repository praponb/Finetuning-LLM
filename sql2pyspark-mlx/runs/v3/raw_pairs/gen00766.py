from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "opened_date",
        "account_type",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "status", "opened_date", "account_type")
)
