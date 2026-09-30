from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("amount", "id")

result = (
    df_payments
    .filter(F.col("status") != "FAILED")
    .select(
        "id",
        "status",
        "amount",
        F.row_number().over(w).alias("amount_row"),
        F.row_number().over(w).alias("row_num"),
    )
)
