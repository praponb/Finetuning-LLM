from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy("opened_date")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "account_type",
        "opened_date",
        F.dense_rank().over(w).alias("dense_rnk"),
    )
)
