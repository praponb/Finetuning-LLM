from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        F.row_number().over(w).alias("rn"),
    )
    .filter(F.col("rn") <= 3)
    .orderBy("start_date", F.col("rn").desc())
)
