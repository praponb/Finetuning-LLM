from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([3, 16, 28, 39]))
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
)
