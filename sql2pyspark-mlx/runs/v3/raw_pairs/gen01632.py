from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "start_date",
        "plan",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 3)
    .orderBy("plan")
)
