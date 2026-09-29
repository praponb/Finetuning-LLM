from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_customers
    .select(
        "id",
        "country",
        "signup_date",
        "credit_limit",
        F.round(F.avg("credit_limit").over(w), 2).alias("credit_limit_moving_avg_4"),
    )
    .orderBy("credit_limit_moving_avg_4")
)
