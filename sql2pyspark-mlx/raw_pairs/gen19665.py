from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
)
