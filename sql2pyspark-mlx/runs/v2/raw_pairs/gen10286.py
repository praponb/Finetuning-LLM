from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .filter(F.col("balance") <= 73000)
    .select(
        "id",
        "customer_id",
        "opened_date",
        "account_type",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 3)
    .select("id", "customer_id", "opened_date", "account_type")
    .orderBy("opened_date")
)
