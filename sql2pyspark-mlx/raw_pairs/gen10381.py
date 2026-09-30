from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method")

result = (
    df_payments
    .filter(F.col("status") == "SUCCESS")
    .select(
        "id",
        "method",
        "amount",
        F.count("*").over(w).alias("method_count"),
        F.max("amount").over(w).alias("method_max_amount"),
    )
)
