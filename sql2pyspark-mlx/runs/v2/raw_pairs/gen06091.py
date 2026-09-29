from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(~F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 1)
    .select("id", "customer_id", "balance", "opened_date")
)
