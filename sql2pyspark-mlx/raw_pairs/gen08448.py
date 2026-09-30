from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "credit_limit",
        F.round(F.avg("credit_limit").over(w), 2).alias("credit_limit_moving_avg_3"),
    )
)
