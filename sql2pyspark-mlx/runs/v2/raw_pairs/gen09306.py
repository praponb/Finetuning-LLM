from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy(F.col("balance").desc(), "id")
w2 = Window.orderBy(F.col("opened_date").desc())

result = (
    df_accounts
    .filter(F.col("balance") > 71000)
    .select(
        "id",
        "account_type",
        "status",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("balance_row"),
        F.rank().over(w2).alias("rnk"),
    )
)
