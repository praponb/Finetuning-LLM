from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("account_type") != "LOAN")
    .select(
        "id",
        "status",
        "balance",
        "account_type",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "status", "balance", "account_type")
)
