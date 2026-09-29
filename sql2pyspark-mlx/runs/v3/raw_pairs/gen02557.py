from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("start_date").desc())

result = (
    df_subscriptions
    .filter(F.year("end_date") == 2023)
    .select(
        "id",
        "plan",
        "end_date",
        "customer_id",
        "start_date",
        F.row_number().over(w).alias("rn"),
        F.dense_rank().over(w2).alias("start_date_dense_rank"),
    )
    .orderBy(F.col("id").desc(), F.col("rn").desc())
)
