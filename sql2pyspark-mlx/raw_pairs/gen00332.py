from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc())

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "customer_id",
        F.dense_rank().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 2)
)
