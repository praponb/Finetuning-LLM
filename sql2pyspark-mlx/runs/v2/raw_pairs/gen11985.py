from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date")

result = (
    df_accounts
    .filter(F.col("account_type") == "CREDIT")
    .select(
        "id",
        "account_type",
        "opened_date",
        "customer_id",
        F.dense_rank().over(w).alias("rn"),
    )
    .filter(F.col("rn") == 1)
    .select("id", "account_type", "opened_date", "customer_id")
    .orderBy("id", "opened_date")
)
