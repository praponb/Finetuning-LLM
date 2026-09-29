from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "end_date",
        "start_date",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
)
