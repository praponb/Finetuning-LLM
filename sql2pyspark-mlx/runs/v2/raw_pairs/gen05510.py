from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("end_date") > "2025-03-16")
    .select(
        "id",
        "monthly_fee",
        "end_date",
        F.first("end_date").over(w).alias("first_end_date"),
    )
)
