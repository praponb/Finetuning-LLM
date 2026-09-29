from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        F.row_number().over(w).alias("end_date_row"),
    )
)
