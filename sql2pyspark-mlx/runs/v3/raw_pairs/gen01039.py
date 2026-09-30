from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("end_date").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") >= 210)
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.row_number().over(w).alias("pos"),
    )
    .filter(F.col("pos") <= 5)
    .orderBy(F.col("end_date").desc())
)
