from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("segment", "city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("city").isin(["London", "Tokyo"]))
    .select(
        "id",
        "city",
        "signup_date",
        "credit_limit",
        "segment",
        F.round(F.avg("credit_limit").over(w), 2).alias("credit_limit_moving_avg_3"),
        F.lag("credit_limit", 1, 0).over(w2).alias("prev_credit_limit"),
    )
)
