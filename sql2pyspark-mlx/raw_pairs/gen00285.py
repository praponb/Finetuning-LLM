from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 2)
    .select("id", "account_type", "balance", "status")
)
