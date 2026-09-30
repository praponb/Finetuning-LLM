from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "account_type").orderBy(F.col("balance").desc(), "id")
w2 = Window.orderBy("balance", "id")
w3 = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id")

valid = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-02-28").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

result = (
    valid
    .filter(F.col("account_type") == "SAVINGS")
    .select(
        "id",
        "account_type",
        "opened_date",
        F.ntile(10).over(w).alias("balance_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w3).alias("row_seq_2"),
    )
)
