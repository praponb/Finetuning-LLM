from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        "status",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") == 1)
    .select("id", "plan", "monthly_fee", "status")
    .orderBy("plan")
)
