from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date")
w2 = Window.partitionBy("status").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        "opened_date",
        "balance",
        F.dense_rank().over(w).alias("opened_date_dense_rank"),
        F.lead("balance", 1).over(w2).alias("next_balance"),
    )
)
