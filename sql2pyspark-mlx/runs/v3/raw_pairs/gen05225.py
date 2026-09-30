from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("status").orderBy("opened_date")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.ntile(10).over(w).alias("bucket"),
        F.dense_rank().over(w2).alias("opened_date_dense_rank"),
    )
)
