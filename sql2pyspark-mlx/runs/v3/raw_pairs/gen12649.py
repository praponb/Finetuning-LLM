from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.col("customer_id") == 35)
    .select(
        "id",
        "account_type",
        "balance",
        "status",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 1)
    .select("id", "account_type", "balance", "status")
    .orderBy(F.col("balance").desc())
)
