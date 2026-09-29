from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").desc(), "id")

result = (
    df_accounts
    .filter(F.col("customer_id").isin([2, 13, 28]))
    .select(
        "id",
        "status",
        "opened_date",
        "customer_id",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 1)
    .select("id", "status", "opened_date", "customer_id")
)
