from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .select(
        "id",
        "status",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("balance_row"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
    .orderBy(F.col("opened_date").desc())
)
